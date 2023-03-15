#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List, Tuple, Union

import pyspark.sql.functions as F

from dataclasses import dataclass, field
from pyspark.sql import DataFrame

import transparency_engine.modules.graph.link_filtering.dynamic_link_scoring as dls
import transparency_engine.pipeline.schemas as schemas

from transparency_engine.analysis.link_filtering.base_link_filter import BaseLinkFilter


logger = logging.getLogger(__name__)


@dataclass
class DynamicLinkFilteringConfig:
    """
    Configuration for DynamicLinkFiltering.

    Attributes:
    ----------
        sync_attributes: List[str], default=[]
            define sync attributes (i.e. attributes that can be shared by 2 entities in the same periods)
        async_attributes: List[str], default=[]
            define async attributes (i.e. attributes that can be shared by 2 entities in different periods)
        min_overall_similarity: float, default=0.1
            the minimum similarity between 2 entities to be considered as a link. For "auto" mode, set the value to None.
        min_sync_similarity: float, default=0.8
            the minimum similarity between 2 entities to be considered as a link in the same period. For "auto" mode, set the value to None.
        min_async_similarity: float, default=0.8
            the minimum similarity between 2 entities to be considered as a link in different periods. For "auto" mode, set the value to None.
        min_normalized_overall_similarity: float, default=None
            the minimum normalized similarity between 2 entities to be considered as a link. For "auto" mode, set the value to None.
        anomaly_threshold: float, default=3.0
            the threshold for anomaly detection.
    """

    sync_attributes: List[str] = field(
        default_factory=lambda: []
    )  # define sync attributes (i.e. attributes that can be shared by 2 entities in the same periods)
    async_attributes: List[str] = field(
        default_factory=lambda: []
    )  # define async attributes (i.e. attributes that can be shared by 2 entities in different periods)
    min_overall_similarity: Union[float, None] = 0.0
    min_sync_similarity: Union[float, None] = 0.8
    min_async_similarity: Union[float, None] = 0.8
    min_normalized_overall_similarity: Union[float, None] = None
    anomaly_threshold: float = 3.0
    attribute_join_token: str = "::"
    edge_join_token: str = "--"


class DynamicIndividualLinkFilter(BaseLinkFilter):
    """
    This filter removes dynamic links between nodes.

    Params:
    -------
        config: The configuration for the filter.
        entity_col: The name of the entity column.
        source_col: The name of the source column.
        target_col: The name of the target column.
        related_col: The name of the related column.
        source_type_col: The name of the source type column.
        target_type_col: The name of the target type column.
        input_time_col: The name of the time column.
    """

    def __init__(
        self,
        data_type: str,
        config: DynamicLinkFilteringConfig,
        multipartite_graph_input: DataFrame,
        entity_col: str = schemas.ENTITY_ID,
        source_col: str = schemas.SOURCE,
        target_col: str = schemas.TARGET,
        related_col: str = schemas.RELATED,
        source_type_col: str = schemas.SOURCE_TYPE,
        target_type_col: str = schemas.TARGET_TYPE,
        input_time_col: str = schemas.TIME_PERIOD,
    ):
        self.data_type = data_type
        self.config = config
        self.multipartite_graph_input = multipartite_graph_input
        self.entity_col = entity_col
        self.source_col = source_col
        self.target_col = target_col
        self.related_col = related_col
        self.source_type_col = source_type_col
        self.target_type_col = target_type_col
        self.input_time_col = input_time_col

    def filter(self, input_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Perform the filtering on the input dataframe and return a new dataframe.

        Params:
        -------
            input_data: Spark DataFrame
                The input dataset to apply filters

        Returns:
        -------
            final_links_df: Spark DataFrame
                The filtered dataset
            multipartite_edge_df: Spark DataFrame
                The multipartite graph edges dataset for the filtered dataset

        """

        input_links_df = dls.mirror_links(input_data, self.source_col, self.target_col)

        filtered_links_df = dls.filter_links(
            source_entity=self.source_col,
            target_entity=self.target_col,
            predicted_links=input_links_df,
            activity_data=self.multipartite_graph_input,
            sync_attributes=self.config.sync_attributes,
            async_attributes=self.config.async_attributes,
            activity_time=self.input_time_col,
            relationship_type=self.data_type,
            min_overall_similarity=self.config.min_overall_similarity,
            min_normalized_overall_similarity=self.config.min_normalized_overall_similarity,
            min_sync_similarity=self.config.min_sync_similarity,
            min_async_similarity=self.config.min_async_similarity,
            anomaly_threshold=self.config.anomaly_threshold,
        )

        final_links_df, multipartite_edge_df = dls.generate_link_output(
            predicted_links=filtered_links_df,
            entity_col=self.entity_col,
            edge_join_token=self.config.edge_join_token,
        )
        return final_links_df, multipartite_edge_df
