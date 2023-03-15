#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import Union

import numpy as np

from scipy import linalg
from scipy.sparse.linalg import svds

from transparency_engine.modules.graph.embed.base_embed import (
    BaseEmbed,
    BaseNodeEmbedding,
)
from transparency_engine.modules.graph.embed.use_graph import (
    UnfoldedGraph,
    UnfoldedGraphNode,
    UnfoldedNodeTypes,
)
from transparency_engine.modules.math.embedding_processing import (
    degree_correct,
    truncate,
)
from transparency_engine.modules.math.matrix_operations import to_laplacian


logger = logging.getLogger(__name__)


class UnfoldedSpectralEmbed(BaseEmbed[UnfoldedGraph]):
    """
    The unfolded spectral embedding is a K-dimensional Euclidean representation
    of an unfolded graph based on its adjacency matrix. It relies on SVD to produce two sets of embeddings:
    - Anchor embeddings: represent the overall behavior of each graph node across all layers (or all timeframes in the case of dynamic graphs)
    - Unfolded embeddings: represent the behavior of each graph node at each layer (or each point in time), with provable
    stability guarantees that constant node behavior at any layer or time results in a constant node position.

    This is an implementation of the Unfolded Spectral Graph Embedding algorithm described in the following papers:
    1. Modell, A., Gallagher, I., Cape, J., & Rubin-Delanchy, P. (2022, arXiv). Spectral embedding and the latent geometry of multipartite networks
    2. Whiteley, N., Gray, A. & Rubin-Delanchy, P. (2021, NeurIPS). Matrix factorisation and the interpretation of geodesic distance
    3. Gallagher, I., Jones, A. & Rubin-Delanchy, P. (2021, NeurIPS). Spectral embedding for dynamic networks with stability guarantees
    """

    def __init__(
        self,
        node_type: UnfoldedNodeTypes = UnfoldedNodeTypes.ANCHOR,
        embedding_dim: Union[int, None] = None,
        use_laplacian_matrix: bool = True,
        regularizer: Union[float, None] = None,
        degree_correct: bool = True,
        truncate_embedding: bool = True,
        elbow_sensitivity: float = 2.0,
    ):
        """
        Initialize the unfolded spectral embedding model.

        Params:
        ---------
            node_type: UnfoldedNodeTypes, default UnfoldedNodeTypes.ANCHOR
                Specify whether the embedding is generated for anchor or unfold graph nodes
            embedding_dim: int, default = None
                The embedding dimension, if None the dimension will be automatically determined using an elbow test
            use_laplacian_matrix: bool, default = True
                If True, convert the original adjacency matrix to laplacian matrix
            regularizer: Union[float, None], default = None
                Regularizer used for converting the adjacency matrix to laplacian matrix. If None, automatically calculate the regularizer value
            degree_correct: bool, default = True
                If True, perform degree correction on the embedding
            truncate_embedding: bool, default = True
                If True, truncate embedding to desired dimension
            elbow_sensitivity: float, default = 2.0
                Sensitivity of the elbow test
        """
        self.node_type = node_type
        self.embedding_dim = embedding_dim
        self.use_laplacian_matrix = use_laplacian_matrix
        self.regularizer = regularizer
        self.degree_correct = degree_correct
        self.truncate_embedding = truncate_embedding
        self.elbow_sensitivity = elbow_sensitivity

    def fit(self, input_data: UnfoldedGraph, **kwargs) -> "UnfoldedSpectralEmbed":
        """
        Compute the embedding vectors from an unfolded graph and transform the data.

        Params:
        ---------
            input_data: UnfoldedGraph
                The unfolded graph object to compute the embeddings on

        Returns:
        ---------
            self: The fitted UnfoldedSpectralEmbed class instance
        """
        unfolded_matrix = input_data.adj_matrix
        if self.use_laplacian_matrix:
            unfolded_matrix = to_laplacian(
                A=unfolded_matrix, regularizer=self.regularizer
            )

        embedding_dim = min(self.embedding_dim, min(unfolded_matrix.shape))
        if embedding_dim > 1:
            u, s, vT = svds(unfolded_matrix, embedding_dim - 1)
            o = np.argsort(s[::-1])

            # get embedding results
            if self.node_type == UnfoldedNodeTypes.ANCHOR:
                # return left embedding
                embedding_vectors = u[:, o] @ np.diag(np.sqrt(s[o]))
            else:
                # return right embedding
                embedding_vectors = vT.T[:, o] @ np.diag(np.sqrt(s[o]))

            # truncate embedding vectors if required
            if self.truncate_embedding:
                embedding_vectors = truncate(
                    embedding_vectors=embedding_vectors,
                    embedding_dim=embedding_dim,
                    singular_values=s[o],
                )
                logger.info(
                    f"Truncated embedding dimension: {embedding_vectors.shape[1]}"
                )
        else:
            embedding_vectors = unfolded_matrix.toarray()

        # perform degree correction on embedding vectors if required
        if self.degree_correct:
            degree_correct(embedding_vectors)

        # get node attributes
        if self.node_type == UnfoldedNodeTypes.ANCHOR:
            node_attributes = [
                node
                for node in input_data.nodes
                if node.node_type == UnfoldedNodeTypes.ANCHOR
            ]
        else:
            node_attributes = [
                node
                for node in input_data.nodes
                if node.node_type == UnfoldedNodeTypes.UNFOLD
            ]
        self.embedding = BaseNodeEmbedding[
            Union[int, float, str, bool], UnfoldedGraphNode
        ](embedding_vectors, node_attributes)
        self.is_fitted = True
        return self

    def transform(self, input_data: UnfoldedGraph, **kwargs) -> "BaseNodeEmbedding":
        """
        Transform out-of-sample data into the embedding space.

        Params:
        ----------
            input_data: UnfoldedGraph
                The out-of-sample graph object to compute the embeddings on

        Returns:
        ----------
            BaseNodeEmbedding The embedding object of the out-of-sample data
        """
        if not self.is_fitted:
            raise ValueError(
                "This UnfoldedSpectralEmbed instance is not fitted yet. Please call 'fit' method first."
            )
        else:
            raise NotImplementedError(
                "Out-of-sample embedding is not currently supported with UnfoldedSpectralEmbed"
            )

    def fit_transform(self, input_data: UnfoldedGraph, **kwargs) -> "BaseNodeEmbedding":
        """
        Compute the embedding vectors from the input data and transform the data.

        Params:
            input_data: BaseGraph
                The graph object to compute the embeddings on

        Returns:
            BaseNodeEmbedding The embedding object of the input data
        """
        self.fit(input_data, **kwargs)
        return self.embedding

    def get_embedding_by_partition(self, partition: str) -> "BaseNodeEmbedding":
        """
        Compute the embedding for a given partition (e.g., entity partition).

        For multipartite graphs, nodes may be divided into different partitions.
        For each partition, we perform a secondary dimentionality reduction step on the initial
        spectral embedding using SVD to recover node representations in a given subspace (i.e. partition).

        Params:
        ----------
            partitions: str
                Name of the partition to extract embedding for

        Returns:
        ----------
            partition_embedding: BaseNodeEmbedding
                The embedding object of the given partition
        """
        if not self.embedding:
            raise ValueError(
                "UnfoldedSpectralEmbed error: embedding object is None. Call fit() to generate the initial spectral embedding first."
            )
        else:
            # get number of partitions
            n_partitions = len(set([node.partition for node in self.embedding.nodes]))

            if n_partitions == 1:
                return self.embedding
            else:
                selected_embedding = self.embedding.select_embedding_by_attribute(
                    attribute_type="partition", attribute_values=partition
                )
                if len(selected_embedding.embedding_vectors) > 1:
                    embedding_vectors = selected_embedding.embedding_vectors
                    u, s, vT = linalg.svd(embedding_vectors, full_matrices=False)
                    o = np.argsort(s[::-1])
                    partition_embedding_vectors = embedding_vectors @ vT.T[:, o]
                    partition_embedding = BaseNodeEmbedding[
                        Union[int, float, str, bool], UnfoldedGraphNode
                    ](partition_embedding_vectors, selected_embedding.nodes)
                    return partition_embedding
                else:
                    return selected_embedding
