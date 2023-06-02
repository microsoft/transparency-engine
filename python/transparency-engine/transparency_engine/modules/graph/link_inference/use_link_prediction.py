#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List, Tuple, Union

from transparency_engine.modules.graph.embed.base_embed import BaseNodeEmbedding
from transparency_engine.modules.graph.embed.use_embedding import UnfoldedSpectralEmbed
from transparency_engine.modules.graph.embed.use_graph import (
    UnfoldedGraph,
    UnfoldedGraphNode,
    UnfoldedNodeTypes,
)
from transparency_engine.modules.graph.link_inference.base_link_prediction import (
    BaseNodeLinkEstimator,
)
from transparency_engine.modules.similarity.ann_search import get_all_closet_pairs


logger = logging.getLogger(__name__)


class USENodeLinkEstimator(BaseNodeLinkEstimator[UnfoldedGraph, UnfoldedGraphNode]):
    """
    This estimator generates anchor or unfolded embedding for each graph node using the Unfolded Spectral Embed method,
    then infers links between graph nodes by searching for N nearest neighbors for each node in the embedding space.
    The estimator infers two types of node links:
    - synchronous links: nearest-neighbor links based on distances of the anchor embeddings. These links represent nodes
    that behave similarly in the same layers/time periods.
    - asynchronous links: nearest-neighbor links based on distances of the unfolded embeddings. These links represent nodes
    that behave similarly in different layers/time periods.
    """

    def __init__(
        self,
        node_type: UnfoldedNodeTypes = UnfoldedNodeTypes.ANCHOR,
        node_partition: Union[str, None] = None,
        n_connected_components: Union[int, None] = None,
        min_component_size: int = 2,
        embedding_dim: Union[int, None] = None,
        use_laplacian_matrix: bool = True,
        regularizer: Union[float, None] = None,
        degree_correct: bool = True,
        truncate_embedding: bool = True,
        elbow_sensitivity: float = 2.0,
        n_neighbors: int = 20,
        ann_list: Union[int, None] = None,
        ann_normalize: bool = True,
        ann_probe: int = 5,
        min_similarity: float = 0.5,
    ):
        """
        Initialize the unfolded spectral embedding model.

        Params:
        ---------
            node_type: UnfoldedNodeTypes, default UnfoldedNodeTypes.ANCHOR
                Specify whether the links are inferred based on the embedding of anchor or unfold graph nodes
            node_partition:  str, default = None
                Partition of the graph nodes (e.g. EntityID) to infer links for. If None, do not project the initial embedding into any partition subspace.
            n_connected_components: Union[int, None]
                Number of connected components to run the link inference on
            min_component_size: int, default = 2
                Min number of nodes in a connected component
            embedding_dim: int, default = None
                The embedding dimension, if None the dimension will be automatically determined using an elbow test
            use_laplacian_matrix: bool, default = True
                If True, convert the original adjacency matrix to laplacian matrix
            regularizer: float, default = 0
                Regularizer used for converting the adjacency matrix to laplacian matrix
            degree_correct: bool, default = True
                If True, perform degree correction on the embedding
            truncate_embedding: bool, default = True
                If True, truncate embedding to desired dimension
            elbow_sensitivity: float, default = 2.0
                Sensitivity of the elbow test
            n_neighbors: int, default = 20
                Maximumn number of nearest neighbors to return for each node
            ann_list: int, default = None
                The number of cells to divide the embedding space into. If None, automatically select the cell number based on the number of embeddings
            ann_normalize: bool, default = True
                If True, perform L2 normalization on the embedding (i.e. using cosine similarity as the distance measure)
            ann_probe: int, default = 5
                Number of nearby cells to search for neighest neighbors.
            min_similarity: float, default = 0.5
                Filter out links with similarity score < min_similarity
        """
        self.node_type = node_type
        self.node_partition = node_partition
        self.n_connected_components = n_connected_components
        self.min_component_size = min_component_size
        self.embedding_dim = embedding_dim
        self.use_laplacian_matrix = use_laplacian_matrix
        self.regularizer = regularizer
        self.degree_correct = degree_correct
        self.truncate_embedding = truncate_embedding
        self.elbow_sensitivity = elbow_sensitivity
        self.n_neighbors = n_neighbors
        self.ann_list = ann_list
        self.ann_normalize = ann_normalize
        self.ann_probe = ann_probe
        self.min_similarity = min_similarity

    def fit(self, input_graph: UnfoldedGraph, **kwargs) -> "BaseNodeLinkEstimator":
        """
        Generate anchor or unfolded embedding for nodes in a given partition.

        Params:
            input_graph: UnfoldedGraph
                The graph object to infer the node links on

        Returns:
            self: The fitted BaseNodeLinkEstimator class instance
        """
        # get connected components for the given partition
        anchor_components, unfold_components = input_graph.connected_components(
            n_components=self.n_connected_components,
            min_component_size=self.min_component_size,
            partition=self.node_partition,
        )
        if self.node_type == UnfoldedNodeTypes.ANCHOR:
            components = anchor_components
        else:
            components = unfold_components
        # get node embedding for each component and index the embedding for ANN search
        component_embeddings = {}
        for component_index, component in enumerate(components):
            logger.info(f'Component ID: {len(component.get_nodes())}')
            embedder = UnfoldedSpectralEmbed(
                node_type=self.node_type,
                embedding_dim=self.embedding_dim,
                use_laplacian_matrix=self.use_laplacian_matrix,
                regularizer=self.regularizer,
                degree_correct=self.degree_correct,
                truncate_embedding=self.truncate_embedding,
                elbow_sensitivity=self.elbow_sensitivity,
            )
            embedder.fit(component)

            embedding = (
                embedder.get_embedding_by_partition(self.node_partition)
                if self.node_partition
                else embedder.embedding
            )

            component_embeddings[component_index] = embedding
        self.embedding = component_embeddings
        self.is_fitted = True
        return self

    def fit_predict(
        self, input_graph: UnfoldedGraph, **kwargs
    ) -> List[Tuple[UnfoldedGraphNode, UnfoldedGraphNode, float]]:
        """
        Generates the embedding for all nodes in the input graph and computes the
        nearest neighbors for each node.

        Params:
            input_graph: UnfoldedGraph
                The input graph to predict node links for

        Returns:
            List[Tuple[UnfoldedGraphNode, UnfoldedGraphNode, float]]
                List of inferred links for all nodes in the graph. Each link is a tuple of (source, target, similar
        """
        # generate embeddings
        self.fit(input_graph, **kwargs)

        # get nearest neighbors for all nodes in each component
        all_links = []
        for component in self.embedding:
            logger.info(f'Component ID: {component}')
            embedding: BaseNodeEmbedding = self.embedding[component]
            inferred_links = get_all_closet_pairs(
                vectors=embedding.embedding_vectors,
                n_list=self.ann_list,
                normalize=self.ann_normalize,
                n_probe=self.ann_probe,
                n_neighbors=self.n_neighbors,
                min_similarity=self.min_similarity,
            ) 

            for link in inferred_links:
                if link[0] < len(embedding.nodes) and link[1] < len(embedding.nodes):
                    source_node: UnfoldedGraphNode = embedding.nodes[link[0]]
                    target_node: UnfoldedGraphNode = embedding.nodes[link[1]]
                    similarity_score: float = link[2]
                    all_links.append((source_node, target_node, similarity_score))

        return all_links

    def predict(
        self, nodes: Union[UnfoldedGraphNode, List[UnfoldedGraphNode]], **kwargs
    ) -> List[Tuple[UnfoldedGraphNode, UnfoldedGraphNode, float]]:
        """
        Infer links for out-of-sample graph nodes.

        Params:
            input_data: Union[UnfoldedGraphNode, List[UnfoldedGraphNode]]
                Single or list of out-of-sample nodes to predict links for

        Returns:
            List[Tuple[UnfoledGraphNode, UnfoledGraphNode, float]]
                List of inferred links for the out of sample nodes. Each link is a tuple of (source, target, similarity_score)
        """
        if not self.is_fitted:
            raise ValueError(
                "This USENodeLinkEstimator instance is not fitted yet. Please call 'fit' method first."
            )
        else:
            raise NotImplementedError(
                "Out-of-sample link inference is not currently supported with USENodeLinkEstimator"
            )
