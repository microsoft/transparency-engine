#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from enum import Enum
from typing import List, Tuple, Union

import numpy as np
import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame, Window
from scipy.sparse import coo_matrix, csr_matrix
from scipy.sparse.csgraph import connected_components

from transparency_engine.modules.graph.embed.base_embed import BaseGraph, BaseGraphNode
from transparency_engine.modules.math.matrix_operations import (
    reweight_matrices_by_norm,
    symmetric_dilation,
)


logger = logging.getLogger(__name__)


class UnfoldedNodeTypes(str, Enum):
    """
    Enum for types of graph nodes in an unfolded graph.
    """

    ANCHOR = "anchor"
    UNFOLD = "unfold"

    @staticmethod
    def from_string(node_type: str):
        """
        Converts a string to an UnfoledNodeTypes enum.

        Parameters
        ----------
        node_type : str
            The string to convert.

        Returns
        -------
        UnfoldedNodeTypes
            The UnfoldedNodeTypes enum.
        """
        if node_type == "anchor":
            return UnfoldedNodeTypes.ANCHOR
        elif node_type == "unfold":
            return UnfoldedNodeTypes.UNFOLD
        else:
            raise ValueError(f"Invalid node type: {node_type}")


@dataclass
class UnfoldedGraphNode(BaseGraphNode):
    """Stores attributes of a node in an unfolded graph."""

    id: int
    partition: str
    node_type: UnfoldedNodeTypes
    unfold_attribute: Union[str, None]


class UnfoldedGraph(BaseGraph[UnfoldedGraphNode]):
    """
    Unfolded graph can be used to represent either a multiplex graph of a dynamic (i.e. time-series) graph.

    In an unfolded graph, we take the adjacency matrix of each component graph (e.g., an individual layer in
    a multiplex graph, or a graph at a given point in time in a dynamic graph).
    We then horizontally concatenate all those matrices into a single matrix as input for node embedding computation.

    This is an implementation of the Unfolded Spectral Graph Embedding algorithm described in the following papers:
    1. Modell, A., Gallagher, I., Cape, J., & Rubin-Delanchy, P. (2022, arXiv). Spectral embedding and the latent geometry of multipartite networks
    2. Whiteley, N., Gray, A. & Rubin-Delanchy, P. (2021, NeurIPS). Matrix factorisation and the interpretation of geodesic distance
    3. Gallagher, I., Jones, A. & Rubin-Delanchy, P. (2021, NeurIPS). Spectral embedding for dynamic networks with stability guarantees
    """

    def __init__(
        self,
        adj_matrix: csr_matrix = csr_matrix([0, 0]),
        nodes: List[UnfoldedGraphNode] = [],
        n_anchor_nodes: int = 0,
        n_unfolded_nodes: int = 0,
        n_unfold_attribute_values: int = 0,
    ):
        super().__init__(adj_matrix, nodes)
        self.n_anchor_nodes = n_anchor_nodes
        self.n_unfolded_nodes = n_unfolded_nodes
        self.n_unfold_attribute_values = n_unfold_attribute_values

    def create_graph(
        self,
        graph_data: DataFrame,
        add_self_loop: bool = False,
        source_col: str = "Source",
        target_col: str = "Target",
        source_type_col: str = "SourceType",
        target_type_col: str = "TargetType",
        unfold_attribute_col: str = "Relationship",
        weight_col: str = "Weight",
        **kwargs,
    ) -> "UnfoldedGraph":
        """
        Create an unfoled graph object given graph edge data.

        Params:
        -------------
            graph_data: Spark DataFrame
                The input dataframe that contains graph edge data with schema [source_col, target_col, source_type_col, target_type_col, unfold_attribute_col, weight_col]
            add_self_loop: bool, default = False
                If true, add self-loops into the edge data
            source_col: str default = 'Source'
                Name of the column that contains the source node
            target_col: str default = 'Target'
                Name of the column that contains the target node
            source_type_col: str default = 'SourceType'
                Name of the column that contains the type (i.e. partition) of the Source node
            target_type_col: str default = 'TargetType'
                Name of the column that contains the type (i.e. partition) of the Target node
            unfold_attribute_col: default = 'Relationship'
                Name of the column that contains the value used for unfolding the graph.
                For multiplex graph, this column could contain the relationship between source-target (e.g. name/email/address).
                For dynamic graph, this column could contain the time periods.
            weight_col: default = 'Weight'
                Name of the column that contains edge weights

        Returns:
        -------------
            self: UnfoldedGraph object
                UnfoldedGraph with populated adjacency matrix and node attributes
        """
        # add self-loop if required
        graph_data = graph_data.select(
            source_col,
            target_col,
            weight_col,
            source_type_col,
            target_type_col,
            unfold_attribute_col,
        )
        if add_self_loop:
            self_loop_source = graph_data.selectExpr(
                source_col,
                f"{source_col} as {target_col}",
                f"1 as {weight_col}",
                source_type_col,
                f"{source_type_col} as {target_type_col}",
                unfold_attribute_col,
            )
            self_loop_target = graph_data.selectExpr(
                f"{target_col} as {source_col}",
                target_col,
                f"1 as {weight_col}",
                f"{target_type_col} as {source_type_col}",
                target_type_col,
                unfold_attribute_col,
            )
            graph_data = (
                graph_data.union(self_loop_source)
                .union(self_loop_target)
                .dropDuplicates()
            )

        # index anchor nodes
        anchor_node_data = (
            graph_data.selectExpr(
                f"{source_col} AS Node", f"{source_type_col} AS Partition"
            )
            .union(
                graph_data.selectExpr(
                    f"{target_col} AS Node", f"{target_type_col} AS Partition"
                )
            )
            .dropDuplicates()
        )
        node_window = Window.orderBy(["Partition", "Node"])
        anchor_node_data = anchor_node_data.withColumn(
            "NodeId", F.row_number().over(node_window) - 1
        ).cache()
        n_anchor_nodes = anchor_node_data.count()
        logger.info(f"number of anchor nodes: {n_anchor_nodes}")
        anchor_node_data.show()

        # index values of the unfolded attribute (could be layers in the multiplex graph or time periods in the dynamic graph)
        unfold_attribute_data = graph_data.select(unfold_attribute_col).dropDuplicates()
        unfold_attribute_window = Window.orderBy(unfold_attribute_col)
        unfold_attribute_data = unfold_attribute_data.withColumn(
            "UnfoldAttributeId", F.row_number().over(unfold_attribute_window) - 1
        )
        n_unfold_attribute_values = unfold_attribute_data.count()
        logger.info(f"number of unfold attribute values: {n_unfold_attribute_values}")

        # add indices to edge data
        graph_data = graph_data.join(
            anchor_node_data.selectExpr(
                f"Node AS {source_col}",
                "NodeId AS SourceId",
                f"Partition AS {source_type_col}",
            ),
            on=[source_col, source_type_col],
            how="inner",
        )
        graph_data = graph_data.join(
            anchor_node_data.selectExpr(
                f"Node AS {target_col}",
                "NodeId AS TargetId",
                f"Partition AS {target_type_col}",
            ),
            on=[target_col, target_type_col],
            how="inner",
        )
        graph_data = graph_data.join(
            unfold_attribute_data, on=unfold_attribute_col, how="inner"
        )

        # create dataframe to fill in the unfolded adjacency matrix
        graph_data = graph_data.withColumn(
            "UnfoldSourceId",
            F.lit(n_anchor_nodes) * F.col("UnfoldAttributeId") + F.col("SourceId"),
        )
        graph_data = graph_data.withColumn(
            "UnfoldTargetId",
            F.lit(n_anchor_nodes) * F.col("UnfoldAttributeId") + F.col("TargetId"),
        )
        graph_data.show(100)
        graph_data = (
            graph_data.selectExpr(
                "SourceId AS RowId",
                "UnfoldTargetId AS ColumnId",
                f"{weight_col} AS Weight",
            )
            .union(
                graph_data.selectExpr(
                    "TargetId AS RowId",
                    "UnfoldSourceId AS ColumnId",
                    f"{weight_col} AS Weight",
                )
            )
            .dropDuplicates()
            .cache()
        )
        logger.info(f"adjacency data count: {graph_data.count()}")
        # convert edge data to scipy sparse matrix
        rows = []
        columns = []
        weights = []
        for record in graph_data.collect():
            rows.append(record["RowId"])
            columns.append(record["ColumnId"])
            weights.append(record["Weight"])

        self.adj_matrix = coo_matrix(
            (weights, (rows, columns)),
            shape=(n_anchor_nodes, n_anchor_nodes * n_unfold_attribute_values),
        ).tocsr()

        # get anchor node attributes
        anchor_nodes = []
        for record in anchor_node_data.collect():
            new_node = UnfoldedGraphNode(
                id=record["NodeId"],
                name=record["Node"],
                partition=record["Partition"],
                node_type=UnfoldedNodeTypes.ANCHOR,
                unfold_attribute=None,
            )
            anchor_nodes.append(new_node)
        anchor_nodes.sort(key=lambda node: node.id)
        self.nodes = anchor_nodes

        # get unfolded node attributes
        unfolded_node_data = anchor_node_data.crossJoin(unfold_attribute_data)
        unfolded_nodes = []
        for record in unfolded_node_data.collect():
            node_id = n_anchor_nodes * int(record["UnfoldAttributeId"]) + int(
                record["NodeId"]
            )
            new_node = UnfoldedGraphNode(
                id=node_id,
                name=record["Node"],
                partition=record["Partition"],
                node_type=UnfoldedNodeTypes.UNFOLD,
                unfold_attribute=record[unfold_attribute_col],
            )
            unfolded_nodes.append(new_node)
        unfolded_nodes.sort(key=lambda node: node.id)
        self.nodes.extend(unfolded_nodes)
        self.n_anchor_nodes = n_anchor_nodes
        self.n_unfolded_nodes = n_anchor_nodes * n_unfold_attribute_values
        self.n_unfold_attribute_values = n_unfold_attribute_values

        return self

    def connected_components(
        self,
        n_components: Union[int, None] = None,
        min_component_size: int = 2,
        partition: Union[str, None] = None,
        **kwargs,
    ) -> Tuple[List["UnfoldedGraph"], List["UnfoldedGraph"]]:
        """
        Return 2 sets of connected components:
        - anchor_components: N largest components that have largest numbers of anchor nodes
        - dynamic_components: N largest components that have largest numbers of unfold nodes

        Parameters
        ----------
            graph: UnfoldedGraph
                The graph to find connected components on
            n_components: int default = None
                The number of components to return. If None, returns all components
            min_component_size: int default = 2
                The minimum size of a component to be considered
            partition: Union[str, None] default = None
                Sort components by the number of nodes that belong to a given partition (e.g. all entity nodes). If None, sort by number of all nodes

        Returns
        -------
            Tuple[List[UnfoldedGraph], List[UnfoldedGraph]] The two sets of connected components
        """
        adj_matrix_dilation = symmetric_dilation(self.adj_matrix)
        _, components = connected_components(adj_matrix_dilation)
        components = [
            components[: self.n_anchor_nodes],
            components[self.n_anchor_nodes :],
        ]
        logger.info(components)
        # find all connected components that have at least one anchor and one unfolded nodes
        n_all_valid_components = min(np.max(components[0]), np.max(components[1])) + 1
        all_connected_components = []
        for i in range(n_all_valid_components):
            row_indices = np.where(components[0] == i)[0]
            column_indices = np.where(components[1] == i)[0]
            all_connected_components.append(
                self.__subgraph(row_indices, column_indices, inplace=False)
            )

        # create two sets of components sorted by either number of anchor or unfolded nodes
        anchor_components = sorted(
            all_connected_components,
            key=lambda component: len(
                component.get_nodes(UnfoldedNodeTypes.ANCHOR, partition)
            )
            if component
            else 0,
            reverse=True,
        )
        unfolded_components = sorted(
            all_connected_components,
            key=lambda component: len(
                component.get_nodes(UnfoldedNodeTypes.UNFOLD, partition)
            )
            if component
            else 0,
            reverse=True,
        )

        # remove components that has less than min number of nodes
        anchor_components = [
            component
            for component in anchor_components
            if component and component.num_nodes() >= min_component_size
        ]
        unfolded_components = [
            component
            for component in unfolded_components
            if component and component.num_nodes() >= min_component_size
        ]
        if n_components is None or n_components >= min(
            len(anchor_components), len(unfolded_components)
        ):
            return anchor_components, unfolded_components
        else:
            return anchor_components[:n_components], unfolded_components[:n_components]

    def get_nodes(
        self,
        node_type: UnfoldedNodeTypes = UnfoldedNodeTypes.ANCHOR,
        partition: Union[str, None] = None,
    ) -> List[UnfoldedGraphNode]:
        """
        Returns all anchor nodes in a given partition.

         Params:
         ----------
            node_type: UnfoldedNodeTypes, default = UnfoledNodeTypes.ANCHOR
                whether the node is anchor or unfolded node
            partition: str, default = None
                The node partition. If None, returns all nodes

        Returns
        -------
            List[UnfoldedGraph]
                Either anchor or unfolded nodes in the given partition
        """
        if not partition:
            return [node for node in self.nodes if node.node_type == node_type]
        else:
            return [
                node
                for node in self.nodes
                if node.node_type == node_type and node.partition == partition
            ]

    def reweight_graph(self, inplace=False) -> "UnfoldedGraph":
        """
        Reweight the unfolded adjacency matrix using Frobenius norm.

         Parameters
        ----------
            inplace : bool, default = False
                If True, returns a new graph object with the reweighted matrix

        Returns
        -------
            UnfoldedGraph
                New graph with reweighted adjacency matrix, or self if reweighing is done in place
        """
        adj_matrices = [
            self.adj_matrix[:, i * self.n_anchor_nodes : (i + 1) * self.n_anchor_nodes]
            for i in range(self.n_unfold_attribute_values)
        ]
        reweighted_matrix = reweight_matrices_by_norm(adj_matrices)
        if inplace:
            self.adj_matrix = reweighted_matrix
            return self
        else:
            reweighted_graph = UnfoldedGraph(
                adj_matrix=reweighted_matrix,
                nodes=self.nodes,
                n_anchor_nodes=self.n_anchor_nodes,
                n_unfolded_nodes=self.n_unfolded_nodes,
                n_unfold_attribute_values=self.n_unfold_attribute_values,
            )
            return reweighted_graph

    def __subgraph(
        self, row_indices: np.ndarray, column_indices: np.ndarray, inplace: bool = False
    ) -> "UnfoldedGraph":
        """
        Return a subgraph of the current graph based on the given indices

        Parameters
        ----------
            idx0 : np.ndarray The indices of the first partition
            idx1 : np.ndarray The indices of the second partition
            inplace : bool Whether to return a new graph or modify the current graph

        Returns
        -------
            UnfoldedGraph The subgraph of the current graph
        """
        subgraph_matrix = self.adj_matrix[np.ix_(row_indices, column_indices)]
        subgraph_nodes = [self.nodes[i] for i in row_indices] + [
            self.nodes[i + self.n_anchor_nodes] for i in column_indices
        ]

        n_anchor_nodes = len(
            [node for node in self.nodes if node.node_type == UnfoldedNodeTypes.ANCHOR]
        )
        n_unfolded_nodes = len(
            [node for node in self.nodes if node.node_type == UnfoldedNodeTypes.UNFOLD]
        )
        if inplace:
            self.adj_matrix = subgraph_matrix
            self.nodes = subgraph_nodes
            self.n_anchor_nodes = n_anchor_nodes
            self.n_unfolded_nodes = n_unfolded_nodes
            return self
        else:
            subgraph = UnfoldedGraph(
                adj_matrix=subgraph_matrix,
                nodes=subgraph_nodes,
                n_anchor_nodes=n_anchor_nodes,
                n_unfolded_nodes=n_unfolded_nodes,
                n_unfold_attribute_values=self.n_unfold_attribute_values,
            )
            return subgraph
