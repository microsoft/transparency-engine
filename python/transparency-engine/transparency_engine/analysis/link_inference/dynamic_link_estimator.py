#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List, Union

import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame

import transparency_engine.pipeline.schemas as schemas

from transparency_engine.analysis.link_inference.base_link_estimator import (
    BaseLinkConfig,
    BaseLinkEstimator,
)
from transparency_engine.modules.graph.embed.use_graph import (
    UnfoldedGraph,
    UnfoldedNodeTypes,
)
from transparency_engine.modules.graph.link_inference.use_link_prediction import (
    USENodeLinkEstimator,
)


logger = logging.getLogger(__name__)


@dataclass
class USEDynamicLinkConfig(BaseLinkConfig):
    """
    Configs for an estimator that uses the unfolded spectral embedding (USE) to predict entity links.
    """

    min_weight: float = 0.0
    reweight_graph: bool = True
    add_self_loop: bool = False
    n_connected_components: Union[int, None] = None
    min_component_size: int = 2
    use_laplacian_matrix: bool = True
    regularizer: Union[float, None] = None
    degree_correct: bool = True
    truncate_embedding: bool = True
    elbow_sensitivity: float = 2.0
    ann_list: Union[int, None] = None
    ann_normalize: bool = True
    ann_probe: int = 10
    sync_embedding_dim: Union[int, None] = 128
    sync_links: int = 20
    sync_min_similarity: float = 0.5
    sync_link_prefix: str = schemas.SYNC_ACTIVITY_PREFIX
    async_embedding_dim: Union[int, None] = 32
    async_links: int = 10
    async_min_similarity: float = 0.5
    async_link_prefix: str = schemas.ASYNC_ACTIVITY_PREFIX


class USEDynamicLinkEstimator(BaseLinkEstimator[USEDynamicLinkConfig]):
    """
    This estimator takes an edge list of a dynamic multipartite graph and computes the anchor and unfolded embeddings
    for each entity in the graph using the unfolded spectral embedding (USE) method.
    The estimator infers two types of node links:
    - synchronous links: nearest-neighbor links based on distances of the anchor embeddings. These links represent nodes
    that behave similarly in the same layers/time periods.
    - asynchronous links: nearest-neighbor links based on distances of the unfolded embeddings. These links represent nodes
    that behave similarly in different time periods.
    """

    def __init__(
        self,
        configs: USEDynamicLinkConfig,
        relationship_family: str,
        entity_partition: str = schemas.ENTITY_ID,
        source_col: str = schemas.SOURCE,
        target_col: str = schemas.TARGET,
        source_type_col: str = schemas.SOURCE_TYPE,
        target_type_col: str = schemas.TARGET_TYPE,
        time_col: str = schemas.TIME_PERIOD,
        weight_col: str = schemas.WEIGHT,
        relationship_col: str = schemas.RELATIONSHIP,
    ):
        """
        Initialize the estimator.

        Params:
        -------------
        configs: USEDynamicLinkConfig
            Configuations of the estimator
        relationship_family: str
            Family of the static relationship (e.g. bid activity)
        entity_partition: str, default = 'EntityID'
            Partition of the entity nodes
        source_col: str default = 'Source'
            Name of the column that contains the source entity node
        target_col: str default = 'Target'
            Name of the column that contains the target entity node
        source_type_col: str default = 'SourceType'
            Name of the column that contains the type (i.e. partition) of the Source node.
        target_type_col: str default = 'TargetType'
            Name of the column that contains the type (i.e. partition) of the Target node.
        unfold_attribute_col: default = 'Relationship'
            Name of the column that contains the entity static attributes used for unfolding the graph. (e.g. name/address)
            For dynamic graph, this column could contain the time periods.
        weight_col: default = 'Weight'
            Name of the column that contains edge weights
        relationship_col: default = 'Relationship'
            Name of the column that contains the relationship type in the output dataframe

        """
        super().__init__(configs)
        self.relationship_family = relationship_family
        self.entity_partition = entity_partition
        self.source_col = source_col
        self.target_col = target_col
        self.source_type_col = source_type_col
        self.target_type_col = target_type_col
        self.time_col = time_col
        self.weight_col = weight_col
        self.relationship_col = relationship_col

    def predict(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        """
        Predict entity links based on entities' dynamic attributes.

        Params:
            input_data: Spark DataFrame
                Contains data related to the edge list of a dynamic multipartite graph,
                with schema [source_col, target_col, source_type_col, target_type_col, time_col, weight_col]

        Returns:
            Spark DataFrame Dataframe containing the predicted links with format [Source, Target, SourceType, TargetType, Weight, Relationship]
        """
        if isinstance(input_data, List):
            raise ValueError(
                "The predict method in the USEDynamicLinkEstimator class only takes a single dataframe as input"
            )

        # create the unfolded graph from the edge list data
        filtered_input_data = input_data.filter(
            F.col(self.weight_col) >= self.configs.min_weight
        ).cache()
        logger.info(f"filtered edge data count: {filtered_input_data.count()}")

        unfolded_graph = UnfoldedGraph().create_graph(
            graph_data=filtered_input_data,
            add_self_loop=self.configs.add_self_loop,
            source_col=self.source_col,
            target_col=self.target_col,
            source_type_col=self.source_type_col,
            target_type_col=self.target_type_col,
            unfold_attribute_col=self.time_col,
            weight_col=self.weight_col,
        )

        if self.configs.reweight_graph:
            unfolded_graph = unfolded_graph.reweight_graph()

        sync_link_df = self.__predict_sync_links(unfolded_graph).cache()
        logger.info(
            f"Initial sync link count for {self.relationship_family}: {sync_link_df.count()}"
        )

        async_link_df = self.__predict_async_links(unfolded_graph).cache()
        logger.info(
            f"Initial async link count for {self.relationship_family}: {async_link_df.count()}"
        )

        predicted_link_df = self.__aggregate_dynamic_links(
            sync_link_df=sync_link_df, async_link_df=async_link_df
        ).cache()
        logger.info(
            f"Final link count for {self.relationship_family}: {predicted_link_df.count()}"
        )
        return predicted_link_df

    def __predict_sync_links(self, unfolded_graph: UnfoldedGraph) -> DataFrame:
        """
        Predict entity-entity synchronous links based on entities' dynamic attributes.

        Params:
            unfolded_graph: UnfoldedGraph
                The unfolded graph object created from the edge list of the dynamic multipartite graph

        Returns:
            Spark DataFrame Dataframe containing the predicted links with format [Source, Target, Relationship, Weight]
        """
        link_estimator = USENodeLinkEstimator(
            node_type=UnfoldedNodeTypes.ANCHOR,
            node_partition=self.entity_partition,
            n_connected_components=self.configs.n_connected_components,
            min_component_size=self.configs.min_component_size,
            embedding_dim=self.configs.sync_embedding_dim,
            use_laplacian_matrix=self.configs.use_laplacian_matrix,
            regularizer=self.configs.regularizer,
            degree_correct=self.configs.degree_correct,
            truncate_embedding=self.configs.truncate_embedding,
            elbow_sensitivity=self.configs.elbow_sensitivity,
            n_neighbors=self.configs.sync_links,
            ann_list=self.configs.ann_list,
            ann_normalize=self.configs.ann_normalize,
            ann_probe=self.configs.ann_probe,
            min_similarity=self.configs.sync_min_similarity,
        )
        predicted_links = link_estimator.fit_predict(unfolded_graph)

        # convert data to spark dataframe
        predicted_link_df = link_estimator.convert_links_to_dataframe(
            inferred_links=predicted_links, node_attributes=None
        )
        predicted_link_df = predicted_link_df.withColumn(
            self.relationship_col,
            F.lit(f"{self.configs.sync_link_prefix}{self.relationship_family}"),
        ).withColumnRenamed("SimilarityScore", self.weight_col)
        predicted_link_df = predicted_link_df.withColumn(
            self.source_type_col, F.lit(self.entity_partition)
        )
        predicted_link_df = predicted_link_df.withColumn(
            self.target_type_col, F.lit(self.entity_partition)
        )
        predicted_link_df = predicted_link_df.select(
            self.source_col,
            self.target_col,
            self.weight_col,
            self.source_type_col,
            self.target_type_col,
            self.relationship_col,
        )
        return predicted_link_df

    def __predict_async_links(self, unfolded_graph: UnfoldedGraph) -> DataFrame:
        """
        Predict entity-entity asynchronous links based on entities' dynamic attributes.

        Params:
            unfolded_graph: UnfoldedGraph
                The unfolded graph object created from the edge list of the dynamic multipartite graph

        Returns:
            Spark DataFrame Dataframe containing the predicted links with format [Source, Target, Relationship, Weight]
        """
        link_estimator = USENodeLinkEstimator(
            node_type=UnfoldedNodeTypes.UNFOLD,
            node_partition=self.entity_partition,
            n_connected_components=self.configs.n_connected_components,
            min_component_size=self.configs.min_component_size,
            embedding_dim=self.configs.async_embedding_dim,
            use_laplacian_matrix=self.configs.use_laplacian_matrix,
            regularizer=self.configs.regularizer,
            degree_correct=self.configs.degree_correct,
            truncate_embedding=self.configs.truncate_embedding,
            elbow_sensitivity=self.configs.elbow_sensitivity,
            n_neighbors=self.configs.async_links,
            ann_list=self.configs.ann_list,
            ann_normalize=self.configs.ann_normalize,
            ann_probe=self.configs.ann_probe,
            min_similarity=self.configs.async_min_similarity,
        )
        predicted_links = link_estimator.fit_predict(unfolded_graph)

        # convert data to spark dataframe
        time_attribute = "unfold_attribute"
        predicted_link_df = link_estimator.convert_links_to_dataframe(
            inferred_links=predicted_links, node_attributes=[time_attribute]
        )
        predicted_link_df = predicted_link_df.filter(
            F.col(f"Source_{time_attribute}") != F.col(f"Target_{time_attribute}")
        )
        predicted_link_df = predicted_link_df.withColumn(
            self.relationship_col,
            F.lit(f"{self.configs.async_link_prefix}{self.relationship_family}"),
        ).withColumnRenamed("SimilarityScore", self.weight_col)

        predicted_link_df = predicted_link_df.withColumn(
            self.source_type_col, F.lit(self.entity_partition)
        )
        predicted_link_df = predicted_link_df.withColumn(
            self.target_type_col, F.lit(self.entity_partition)
        )
        predicted_link_df = predicted_link_df.select(
            self.source_col,
            self.target_col,
            self.weight_col,
            self.source_type_col,
            self.target_type_col,
            self.relationship_col,
        )
        return predicted_link_df

    def __aggregate_dynamic_links(
        self, sync_link_df: DataFrame, async_link_df: DataFrame
    ) -> DataFrame:
        """
        Combine sync and async links into one table
        """
        # filter out links already on the sync similarity table
        async_link_df = async_link_df.join(
            sync_link_df.select([self.source_col, self.target_col]),
            on=[self.source_col, self.target_col],
            how="leftanti",
        )
        async_link_df = (
            async_link_df.groupBy(
                [
                    self.source_col,
                    self.target_col,
                    self.relationship_col,
                    self.source_type_col,
                    self.target_type_col,
                ]
            )
            .agg(F.max(self.weight_col).alias(self.weight_col))
            .cache()
        )
        logger.info(f"filtered async link count: {async_link_df.count()}")

        # aggregate links
        all_link_df = sync_link_df.select(
            [
                self.source_col,
                self.target_col,
                self.weight_col,
                self.source_type_col,
                self.target_type_col,
                self.relationship_col,
            ]
        ).union(
            async_link_df.select(
                [
                    self.source_col,
                    self.target_col,
                    self.weight_col,
                    self.source_type_col,
                    self.target_type_col,
                    self.relationship_col,
                ]
            )
        )
        return all_link_df
