#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from abc import abstractmethod
from typing import Dict, Generic, List, Tuple, Union

import numpy as np

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transparency_engine.modules.graph.embed.base_embed import (
    BaseNodeEmbedding,
    Graph,
    GraphNode,
)
from transparency_engine.spark.utils import spark


logger = logging.getLogger(__name__)


class BaseNodeLinkEstimator(Generic[Graph, GraphNode]):
    """Base class for a model that infer links between nodes in a graph based on node embeddings."""

    def __init__(self, embedding: Dict[int, BaseNodeEmbedding] = {}):
        """
        Initialize the link prediction model.

        Params:
        ---------
            embedding: Dict[int, BaseNodeEmbedding]
                embedding dict stores embedding objects for each connected component in the graph
        """
        self.embedding = embedding
        self.is_fitted = False

    @abstractmethod
    def fit(self, input_graph: Graph, **kwargs) -> "BaseNodeLinkEstimator":
        """
        Fit the model to perform link inference.

        Params:
            input_graph: Graph
                The graph object to infer the node links on

        Returns:
            self: The fitted BaseNodeLinkEstimator class instance
        """
        pass

    @abstractmethod
    def predict(
        self, nodes: Union[GraphNode, List[GraphNode]], **kwargs
    ) -> List[Tuple[GraphNode, GraphNode, float]]:
        """
        Infer links for out-of-sample graph nodes.

        Params:
            input_data: Union[GraphNode, List[GraphNode]]
                Single or list of out-of-sample nodes to predict links for

        Returns:
            List[Tuple[GraphNode, GraphNode, float]]
                List of inferred links for the out of sample nodes. Each link is a tuple of (source, target, similarity_score)
        """
        pass

    def fit_predict(
        self, input_graph: Graph, **kwargs
    ) -> List[Tuple[GraphNode, GraphNode, float]]:
        """
        Fit the link inference model and predict links for all nodes in the graph.

        Params:
            input_graph: Graph
                The input graph to predict node links for

        Returns:
            List[Tuple[GraphNode, GraphNode, float]]
                List of inferred links for all nodes in the graph. Each link is a tuple of (source, target, similarity_score)
        """
        self.fit(input_graph, **kwargs)
        return self.predict(input_graph.nodes, **kwargs)

    def convert_links_to_dataframe(
        self,
        inferred_links: List[Tuple[GraphNode, GraphNode, float]],
        node_attributes: Union[List[str], None] = None,
        mirror_link: bool = False,
        source_col: str = "Source",
        target_col: str = "Target",
    ) -> DataFrame:
        """
        Write a list of inferred links to a Spark dataframe, together with selected graph node attributes.

        Params:
            inferred_links: List[Tuple[GraphNode, GraphNode, float]]
                List of inferred links. Each link is a tuple of (source, target, similarity_score)
            node_attributes: Union[List[str], None], default = None
                Names of node attributes to be included in the dataframe
            mirror_link: bool, default = False
                If True, mirror the links (i.e. including both [Source, Target] and [Target, Source] links).
                If False, only include links where Source < Target.

        Returns:
            link_df: Spark DataFrame
                The dataframe with schema [Source, Target, SimilarityScore, Source_Attr1...,SourceAttrN, TargetAttr1,...TargetAttrN]
        """
        link_rows = []

        mirrored_links = []
        for link in inferred_links:
            mirrored_links.append(link)
            mirrored_links.append((link[1], link[0], link[2]))

        # build data schema that include attributes for source and target nodes
        data_schema = [source_col, target_col, "SimilarityScore"]
        if node_attributes is not None:
            attributes = [
                attribute for attribute in node_attributes if attribute != "name"
            ]
            source_attribute_columns = [f"Source_{attr}" for attr in attributes]
            target_attribute_columns = [f"Target_{attr}" for attr in attributes]
            data_schema.extend(source_attribute_columns)
            data_schema.extend(target_attribute_columns)

        for link in mirrored_links:
            source_node: GraphNode = link[0]
            target_node: GraphNode = link[1]
            similarity_score: float = link[2]
            row_data = [source_node.name, target_node.name, similarity_score]
            if node_attributes is not None:
                source_attribute_values = [
                    getattr(source_node, attr) for attr in attributes
                ]
                row_data.extend(source_attribute_values)
                target_attribute_values = [
                    getattr(target_node, attr) for attr in attributes
                ]
                row_data.extend(target_attribute_values)
            link_rows.append(row_data)

        link_df = spark.createDataFrame(link_rows, schema=data_schema).dropDuplicates()
        if not mirror_link:
            link_df = link_df.filter(col(source_col) < col(target_col))
        return link_df

    def convert_embeddings_to_dataframe(
        self, node_attributes: Union[List[str], None] = None
    ) -> DataFrame:
        """
        Write node embeddings to a Spark dataframe, together with selected node attributes.

        Params:
            component_embeddings:  Dict[int, BaseNodeEmbedding]
                The dictionary that contains embedding object for each connected component in a graph
            node_attributes: Union[List[str], None], default = None
                Names of node attributes to be included in the dataframe

        Returns:
            embedding_df: Spark DataFrame
                The dataframe with schema [Node, Component, Embedding]
        """
        # build data schema
        data_schema = ["Node", "Component", "Embedding"]
        if node_attributes is not None:
            attributes = [
                attribute for attribute in node_attributes if attribute != "name"
            ]
            data_schema.extend(attributes)

        embedding_rows = []
        for component in self.embedding:
            embedding_vectors: np.ndarray = self.embedding[component].embedding_vectors
            nodes: List[GraphNode] = self.embedding[component].nodes
            for node, embedding in zip(nodes, embedding_vectors):
                row_data = [node.name, component, embedding.tolist()]
                if node_attributes is not None:
                    attribute_values = [getattr(node, attr) for attr in attributes]
                    row_data.extend(attribute_values)
                embedding_rows.append(row_data)
        embedding_df = spark.createDataFrame(embedding_rows, schema=data_schema)
        return embedding_df
