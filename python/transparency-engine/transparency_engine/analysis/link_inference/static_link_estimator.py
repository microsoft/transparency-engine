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
from transparency_engine.modules.similarity.similarity_score import (
    cosine_similarity_udf,
)


logger = logging.getLogger(__name__)


@dataclass
class USEStaticLinkConfig(BaseLinkConfig):
    """
    Configs for an estimator that uses the unfolded spectral embedding (USE) to predict entity links.
    """

    min_weight: float = 0.0
    reweight_graph: bool = True
    add_self_loop: bool = True
    n_connected_components: Union[int, None] = None
    min_component_size: int = 2
    embedding_dim: Union[int, None] = 128
    use_laplacian_matrix: bool = True
    regularizer: Union[float, None] = None
    degree_correct: bool = True
    truncate_embedding: bool = True
    elbow_sensitivity: float = 2.0
    ann_list: Union[int, None] = None
    ann_normalize: bool = True
    ann_probe: int = 10
    min_similarity: float = 0.5
    add_direct_links: bool = True
    direct_link_min_weight: float = 0.5


class USEStaticLinkEstimator(BaseLinkEstimator[USEStaticLinkConfig]):
    """
    This estimator takes an edge list of an entity-entity multiplex graph and computes the anchor embedding
    for each entity in the graph using the unfolded spectral embedding method. The anchor embedding reperesents
    the entity's overall behavior across the layers of the multiplex graph, and is then used to compute N nearest
    neighbors for each entity in the embedding space.
    """

    def __init__(
        self,
        configs: USEStaticLinkConfig,
        relationship_family: str,
        source_col: str = schemas.SOURCE,
        target_col: str = schemas.TARGET,
        source_type_col: str = schemas.SOURCE_TYPE,
        target_type_col: str = schemas.TARGET_TYPE,
        relationship_col: str = schemas.RELATIONSHIP,
        weight_col: str = schemas.WEIGHT,
    ):
        """
        Initialize the estimator.

        Params:
        -------------
        configs: USEStaticLinkConfig
            Configuations of the estimator
        relationship_family: str
            Family of the static relationship (e.g. contact or ownership)
        source_col: str default = 'Source'
            Name of the column that contains the source entity node
        target_col: str default = 'Target'
            Name of the column that contains the target entity node
        source_type_col: str default = 'SourceType'
            Name of the column that contains the type (i.e. partition) of the Source node.
        target_type_col: str default = 'TargetType'
            Name of the column that contains the type (i.e. partition) of the Target node.
        relationship_col: default = 'Relationship'
            Name of the column that contains the entity static attributes used for unfolding the graph. (e.g. name/address)
         weight_col: default = 'Weight'
            Name of the column that contains edge weights

        """
        super().__init__(configs)
        self.relationship_family = relationship_family
        self.source_col = source_col
        self.target_col = target_col
        self.source_type_col = source_type_col
        self.target_type_col = target_type_col
        self.relationship_col = relationship_col
        self.weight_col = weight_col

    def predict(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        """
        Predict entity links based on entities' static attributes.

        Params:
            input_data: Spark DataFrame
                Contains data related to the edge list of a multiplex graph,
                with schema [source_col, target_col, source_type_col, target_type_col, relationship_col, weight_col]

        Returns:
            Spark DataFrame Dataframe containing the predicted links with format [Source, Target, SourceType, TargetType, Weight, Relationship]
        """
        if isinstance(input_data, List):
            raise ValueError(
                "The predict method in the USEStaticLinkEstimator class only takes a single dataframe as input"
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
            unfold_attribute_col=self.relationship_col,
            weight_col=self.weight_col,
        )

        if self.configs.reweight_graph:
            unfolded_graph = unfolded_graph.reweight_graph()

        # predict links
        link_estimator = USENodeLinkEstimator(
            node_type=UnfoldedNodeTypes.ANCHOR,
            node_partition=None,
            n_connected_components=self.configs.n_connected_components,
            min_component_size=self.configs.min_component_size,
            embedding_dim=self.configs.embedding_dim,
            use_laplacian_matrix=self.configs.use_laplacian_matrix,
            regularizer=self.configs.regularizer,
            degree_correct=self.configs.degree_correct,
            truncate_embedding=self.configs.truncate_embedding,
            elbow_sensitivity=self.configs.elbow_sensitivity,
            n_neighbors=self.configs.n_links,
            ann_list=self.configs.ann_list,
            ann_normalize=self.configs.ann_normalize,
            ann_probe=self.configs.ann_probe,
            min_similarity=self.configs.min_similarity,
        )
        predicted_links = link_estimator.fit_predict(unfolded_graph)

        # convert data to spark dataframe
        predicted_link_df = link_estimator.convert_links_to_dataframe(
            inferred_links=predicted_links, node_attributes=None
        )
        if self.configs.add_direct_links:
            # get candidate direct links
            direct_link_df = input_data.filter(
                F.col(self.weight_col) >= self.configs.direct_link_min_weight
            )
            direct_link_df = (
                direct_link_df.select([self.source_col, self.target_col])
                .dropDuplicates()
                .cache()
            )
            logger.info(f"Total direct links: {direct_link_df.count()}")

            missing_link_df = direct_link_df.join(
                predicted_link_df, on=[self.source_col, self.target_col], how="leftanti"
            )
            missing_link_df = missing_link_df.selectExpr(
                self.source_col, self.target_col
            ).dropDuplicates()
            logger.info(f"Missing direct links: {missing_link_df.count()}")

            # get embedding data
            embedding_data = link_estimator.convert_embeddings_to_dataframe(
                node_attributes=None
            )

            # add direct links
            missing_link_df = self.__add_direct_links(
                direct_links=missing_link_df, embedding_data=embedding_data
            ).cache()
            logger.info(f"Missing links to be added: {missing_link_df.count()}")
            predicted_link_df = predicted_link_df.union(missing_link_df)

        # add source type and target type
        node_type_df = input_data.select(
            [
                self.source_col,
                self.target_col,
                self.source_type_col,
                self.target_type_col,
            ]
        ).dropDuplicates()
        predicted_link_df = predicted_link_df.join(
            node_type_df, on=[self.source_col, self.target_col], how="inner"
        )
        predicted_link_df = predicted_link_df.withColumn(
            self.relationship_col, F.lit(self.relationship_family)
        ).withColumnRenamed("SimilarityScore", self.weight_col)
        predicted_link_df = predicted_link_df.select(
            self.source_col,
            self.target_col,
            self.source_type_col,
            self.target_type_col,
            self.weight_col,
            self.relationship_col,
        ).cache()
        logger.info(
            f"Final link count for {self.relationship_family}: {predicted_link_df.count()}"
        )
        return predicted_link_df

    def __add_direct_links(
        self, direct_links: DataFrame, embedding_data: DataFrame
    ) -> DataFrame:
        """
        Add missing direct links (i.e. pairs of nodes that are connected by a direct edge in the input graph
        with edge weight execeeding a given threshold). The direct links are further filtered by an embedding-based distance measure.

        Params:
        ------------
            direct_links: DataFrame
                Contains the direct links with schema [source_col, target_col]
            embedding_data: DataFrame
                Dataframe that contains the node embeddings, with schema [Node, Component, Embedding]

        Returns:
        ------------

        """
        # compute similarity score for missing links
        source_embedding = embedding_data.selectExpr(
            f"Node as {self.source_col}",
            "Embedding as SourceEmbedding",
            "Component as SourceComponent",
        )
        target_embedding = embedding_data.selectExpr(
            f"Node as {self.target_col}",
            "Embedding as TargetEmbedding",
            "Component as TargetComponent",
        )
        direct_link_scores = direct_links.join(
            source_embedding, on=self.source_col, how="inner"
        )
        direct_link_scores = direct_link_scores.join(
            target_embedding, on=self.target_col, how="inner"
        )
        direct_link_scores = direct_link_scores.filter(
            F.col("SourceComponent") == F.col("TargetComponent")
        )
        direct_link_scores = direct_link_scores.withColumn(
            "SimilarityScore",
            cosine_similarity_udf("SourceEmbedding", "TargetEmbedding"),
        )
        direct_link_scores = direct_link_scores.filter(
            F.col("SimilarityScore") >= self.configs.min_similarity
        ).select(self.source_col, self.target_col, "SimilarityScore")
        return direct_link_scores
