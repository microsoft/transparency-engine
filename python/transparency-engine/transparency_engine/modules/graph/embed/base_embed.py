#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from abc import abstractmethod
from typing import Generic, List, Tuple, TypeVar, Union

import numpy as np

from dataclasses import dataclass
from pyspark.sql import DataFrame
from scipy.sparse import csr_matrix


logger = logging.getLogger(__name__)


@dataclass
class BaseGraphNode:
    """
    Base class to store attributes of a node in a graph.
    Each node has a name as a unique identifier.
    """

    name: Union[int, str]


GraphNode = TypeVar("GraphNode", bound=BaseGraphNode)


class BaseGraph(Generic[GraphNode]):
    """Base class for a graph object (e.g., unipartite, multipartite, multiplex or dynamic graph)"""

    def __init__(
        self,
        adj_matrix: csr_matrix = csr_matrix([0, 0]),
        nodes: List[GraphNode] = [],
    ):
        """
        Initialize a graph object.

        Params:
        ----------
            adj_matrix: csr_matrix
                Sparse adjacency matrix storing graph edge data
            nodes: List
                Contains all nodes in a graph and the associated node attributes

        """
        self.adj_matrix = adj_matrix
        self.nodes = nodes

    def num_nodes(self) -> int:
        """Return number of nodes in the graph."""
        return len(self.nodes)

    @abstractmethod
    def create_graph(self, graph_data: DataFrame, **kwargs) -> "BaseGraph":
        """
        Create a graph object given the input dataframe.

        Params:
        ----------
            graph_data: Spark DataFrame The input dataset that contains graph structure data

        Returns:
            self: BaseGraph Graph object
        """
        return self

    @abstractmethod
    def connected_components(
        self,
        n_components: Union[int, None] = None,
        min_component_size: int = 2,
        **kwargs,
    ) -> Union[List, Tuple[List, List]]:
        """Returns N largest connected components in the graph."""
        pass


NodeAttributeValue = TypeVar("NodeAttributeValue", bound=Union[int, float, str, bool])


class BaseNodeEmbedding(Generic[NodeAttributeValue, GraphNode]):
    """Base class that represents results of an embedding model."""

    def __init__(self, embedding_vectors: np.ndarray, nodes: List[GraphNode]):
        self.embedding_vectors = embedding_vectors
        self.nodes = nodes

    def select_embedding_by_attribute(
        self,
        attribute_type: str,
        attribute_values: Union[NodeAttributeValue, List[NodeAttributeValue]],
    ) -> "BaseNodeEmbedding":
        """
        Returns a new embedding object filtered on a given attribute.

        Params:
        ----------
            attribute_type: str
                Name of the node attribute to filter the embedding on
            attribute_values: List
                List of attribute values to filter by

        Returns:
        ----------
            selected_embedding: BaseNodeEmbedding
                The filtered embedding object
        """
        if not isinstance(attribute_values, List):
            attribute_values = [attribute_values]

        # get indices of the selected nodes
        logger.info(f"Number of nodes: {len(self.nodes)}")
        logger.info(f"Number of vectors: {len(self.embedding_vectors)}")
        selected_node_indices = [
            index
            for index, node in enumerate(self.nodes)
            if getattr(node, attribute_type) in attribute_values
        ]

        selected_embedding_vectors = self.embedding_vectors[selected_node_indices, :]
        selected_nodes = [self.nodes[i] for i in selected_node_indices]
        selected_embedding = BaseNodeEmbedding[NodeAttributeValue, GraphNode](
            selected_embedding_vectors, selected_nodes
        )
        return selected_embedding


Graph = TypeVar("Graph", bound=BaseGraph)


class BaseEmbed(Generic[Graph]):
    """Base class for a model that embeds a graph object to create embedding for graph nodes."""

    def __init__(self):
        """Initialize an embedding model."""
        self.is_fitted = False

    @abstractmethod
    def fit(self, input_data: Graph, **kwargs) -> "BaseEmbed":
        """
        Compute the embedding vectors for input_data.

        Params:
            input_data: BaseGraph
                The graph object to compute the embeddings on

        Returns:
            self: The fitted BaseEmbed class instance
        """
        pass

    @abstractmethod
    def transform(self, input_data: Graph, **kwargs) -> "BaseNodeEmbedding":
        """
        Transform out-of-sample data into the embedding space.

        Params:
            input_data: BaseGraph
                The out-of-sample graph object to compute the embeddings on

        Returns:
            BaseNodeEmbedding The embedding object of the out-of-sample data
        """
        pass

    @abstractmethod
    def fit_transform(self, input_data: Graph, **kwargs) -> "BaseNodeEmbedding":
        """
        Fit the embedding model with the input data and transform the input data to generate embedding.

        Params:
            input_data: BaseGraph
                The graph object to compute the embeddings on

        Returns:
            BaseNodeEmbedding The embedding object of the input data
        """
        pass
