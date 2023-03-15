#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List, Union

import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from transparency_engine.base_transformer import BaseTransformer
from transparency_engine.pipeline.schemas import (
    CATEGORY,
    DESCRIPTION,
    ENTITY_ID,
    EVIDENCE,
    IS_SUPPORTING_FLAG,
    NAME,
    REVIEW_FLAG_ID,
    REVIEW_FLAG_WEIGHT,
)


logger = logging.getLogger(__name__)


@dataclass
class FlagFilterConfig:
    """
    Params:
        min_flag_categories: int, default = 2
            If an entity has a supporting review flag (noisy flag), this flag needs to be accompanied by a min number of
            distinct flag categories to be counted as a valid flag.
    """

    min_flag_categories: int = 2


class FlagFilterTransformer(BaseTransformer):
    """
    Transformer that converts a static or dynamic relationship dataframe into an edge list of a multipartite graph
    """

    def __init__(
        self,
        configs: FlagFilterConfig,
        flag_metadata: Union[DataFrame, None],
        entity_col: str = ENTITY_ID,
        flag_col: str = REVIEW_FLAG_ID,
        supporting_flag_col: str = IS_SUPPORTING_FLAG,
    ):
        """
        Params:
            configs:  FlagConfig
                Contains multipartite graph configuration for each attribute
            flag_metadata: DataFrame
                Contains fuzzy matching results with schema [Source, Target, fuzzy_match_weight_col, attribute_col]
            entity_col: str, default = 'EntityID'
                Name of the column that contains entity ids in the input dataframe
            attribute_col: str, default = 'AttributeID'
                Name of the column that contains different types of text attribute (e.g. name, address)
            value_col: str, default = 'Value'
                Name of column that contain attribute values
            time_col: str, default = 'TimePeriod'
                Name of column that contain time data in the case of a dynamic relationship (set to None for static relationship)
        """
        super().__init__(configs)
        self.flag_metadata = flag_metadata
        self.entity_col = entity_col
        self.flag_col = flag_col
        self.supporting_flag_col = supporting_flag_col

    def transform(self, input_data: DataFrame) -> DataFrame:
        """
        Convert static relationship input data into graph edges of a multiplex graph,
        taking into account both exact matching and fuzzy matching

        Params:
            input_data: Spark DataFrame
                Entity's review flag data with schema [EntityID, FlagID, Evidence]

        Returns:
            Spark DataFrame Filtered entity flag data with schema [EntityID, FlagID, Evidence]
        """
        if (
            self.flag_metadata is None
            or self.supporting_flag_col not in self.flag_metadata.columns
        ):
            return input_data
        else:
            entity_flag_data = input_data.join(
                self.flag_metadata, on=REVIEW_FLAG_ID, how="inner"
            )
            category_data = entity_flag_data.groupby(ENTITY_ID).agg(
                F.collect_set(CATEGORY).alias("categories")
            )
            entity_flag_data = entity_flag_data.join(
                category_data, on=ENTITY_ID, how="inner"
            )
            entity_flag_data = entity_flag_data.withColumn(
                "is_valid_flag",
                _is_valid_flag_udf(
                    F.col(IS_SUPPORTING_FLAG),
                    F.col("categories"),
                    F.lit(self.configs.min_flag_categories),
                ),
            )
            entity_flag_data = entity_flag_data.filter(F.col("is_valid_flag") == 1)
            entity_flag_data = entity_flag_data.select(
                ENTITY_ID, REVIEW_FLAG_ID, EVIDENCE
            ).cache()
            logger.info(f"Filtered entity flag records: {entity_flag_data.count()}")
            return entity_flag_data


@udf(returnType=IntegerType())
def _is_valid_flag_udf(
    is_supporting_flag: bool, flag_categories: List[str], min_categories: int = 2
):
    """
    Helper UDF to check if a flag is a valid flag
    """
    if is_supporting_flag and len(flag_categories) < min_categories:
        return 0
    else:
        return 1


def generate_flag_metadata(entity_flag_data: DataFrame) -> DataFrame:
    """
    If no initial metadata is provided, create a default metadata using
    the flag ids from the entity_flag data.
    Params:
        entity_flag_data: DataFrame
            Entity's review flag data with schema [EntityID, FlagID, Evidence]
    Returns:
        flag_metadata: DataFrame
            Flag metadata with all flag ids found in the entity flag data.
    """
    flag_metadata = entity_flag_data.select(REVIEW_FLAG_ID).dropDuplicates()
    flag_metadata = (
        flag_metadata.withColumn(CATEGORY, F.col(REVIEW_FLAG_ID))
        .withColumn(NAME, F.col(REVIEW_FLAG_ID))
        .withColumn(DESCRIPTION, F.col(REVIEW_FLAG_ID))
    )
    flag_metadata = flag_metadata.withColumn(IS_SUPPORTING_FLAG, F.lit(False))
    flag_metadata = flag_metadata.withColumn(REVIEW_FLAG_WEIGHT, F.lit(1.0))
    return flag_metadata


def validate_flag_metadata(flag_metadata: DataFrame) -> DataFrame:
    """
    Check flag metadata to ensure column, name and description fields are populated.

    Params:
        flag_metadata: DataFrame
            Contains definitions of entity review flags,
            with schema [ENTITY_ID, CATEGORY, NAME, DESCRIPTION, IS_SUPPORTING_FLAG, REVIEW_FLAG_WEIGHT]

    Returns:
        DataFrame: updated flag metadata
    """
    columns = [CATEGORY, NAME, DESCRIPTION]
    for column in columns:
        flag_metadata = flag_metadata.withColumn(
            column,
            F.when(
                (F.col(column).isNull()) | (F.trim(F.col(column)) == ""),
                F.col(REVIEW_FLAG_ID),
            ).otherwise(F.col(column)),
        )
    return flag_metadata
