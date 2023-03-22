#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
import logging

from itertools import chain
from typing import List

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType

from transparency_engine.analysis.scoring.measures import (
    EntityMeasures,
    MeasureConfig,
    NetworkMeasureCategories,
    NetworkMeasures,
    NormalizationTypes,
    ScoringConfig,
    ScoringTypes,
    compute_score,
)
from transparency_engine.modules.stats.aggregation import (
    normalize_max_scale,
    normalize_rank,
)
from transparency_engine.modules.stats.arithmetic_operators import ArithmeticOperators
from transparency_engine.pipeline.schemas import (
    ASYNC_ACTIVITY_PREFIX,
    ENTITY_ID,
    PATHS,
    SOURCE_NODE,
    SYNC_ACTIVITY_PREFIX,
    TARGET_NODE,
)


logger = logging.getLogger(__name__)


DEFAULT_NETWORK_SCORE_CONFIGS: ScoringConfig = ScoringConfig(
    selected_measures=[
        MeasureConfig(
            NetworkMeasures.TARGET_ENTITY_SCORE,
            weight=0.5,
            normalization=NormalizationTypes.BY_MAX_VALUE,
        ),
        MeasureConfig(
            NetworkMeasures.RELATED_ENTITY_SCORE,
            weight=0.5,
            normalization=NormalizationTypes.BY_MAX_VALUE,
        ),
    ],
    scoring_function=ArithmeticOperators.SUM,
)


def compute_network_score(  # nosec - B107
    entity_score_data: DataFrame,
    predicted_link_data: DataFrame,
    configs: ScoringConfig = DEFAULT_NETWORK_SCORE_CONFIGS,
    attribute_join_token: str = "::",
) -> DataFrame:
    """
    Compute all network measures as specified in the NetworkMeasures enum,
    then combine these measures to compute a final network score using the scoring function
    specified in the ScoringConfig object.

    Params:
    --------------
        entity_score_data: DataFrame
            Contains measures and score of individual entities
        predicted_link_data: DataFrame
            Dataframe storing predicted link data, with columns [Source, Target, Paths]
        configs: ScoringConfig
            Configuration of the entity scoring function
        attribute_join_token: str, default = '::'
            Token that join attribute::value pairs

    Returns:
    ---------------
        DataFrame: dataframe with computed entity measures and score (raw and normalized)
    """
    # get target entity measures
    network_measure_data = entity_score_data.selectExpr(
        f"{ENTITY_ID}",
        f"{EntityMeasures.FLAG_COUNT} AS {NetworkMeasures.TARGET_FLAG_COUNT}",
        f"{EntityMeasures.FLAG_WEIGHT} AS {NetworkMeasures.TARGET_FLAG_WEIGHT}",
        f"{EntityMeasures.ENTITY_WEIGHT} AS {NetworkMeasures.TARGET_ENTITY_WEIGHT}",
        f"{ScoringTypes.FINAL_ENTITY_SCORE} AS {NetworkMeasures.TARGET_ENTITY_SCORE}",
    )
    network_measure_data = network_measure_data.withColumn(
        "TARGET_FLAGGED_COUNT",
        F.when(F.col(NetworkMeasures.TARGET_FLAG_COUNT) > 0, 1).otherwise(0),
    )
    network_measure_data = network_measure_data.withColumn(
        "TARGET_FLAGGED_WEIGHT",
        F.when(
            F.col(NetworkMeasures.TARGET_FLAG_COUNT) > 0,
            F.col(NetworkMeasures.TARGET_ENTITY_WEIGHT),
        ).otherwise(0),
    )

    flag_degree_data = __compute_flag_degree(
        entity_score_data=entity_score_data,
        related_entity_data=predicted_link_data,
        attribute_join_token=attribute_join_token,
    )
    network_measure_data = network_measure_data.join(
        flag_degree_data, on=ENTITY_ID, how="left"
    )

    # get directly-related entity measures
    link_data = predicted_link_data.withColumn(
        "is_directly_related", direct_related_entity_udf(PATHS)
    )
    direct_link_data = (
        link_data.filter(F.col("is_directly_related") == 1)
        .select(SOURCE_NODE, TARGET_NODE)
        .dropDuplicates()
    )

    direct_measure_data = __compute_related_entity_measures(
        entity_score_data=entity_score_data,
        related_entity_data=direct_link_data,
        measure_category=NetworkMeasureCategories.DIRECT,
    )
    network_measure_data = network_measure_data.join(
        direct_measure_data, on=ENTITY_ID, how="left"
    )

    # get directly-related entity measures
    indirect_link_data = (
        link_data.filter(F.col("is_directly_related") == 0)
        .select(SOURCE_NODE, TARGET_NODE)
        .dropDuplicates()
    )
    indirect_measure_data = __compute_related_entity_measures(
        entity_score_data=entity_score_data,
        related_entity_data=indirect_link_data,
        measure_category=NetworkMeasureCategories.INDIRECT,
    )
    network_measure_data = network_measure_data.join(
        indirect_measure_data, on=ENTITY_ID, how="left"
    )

    # get related entity measures
    network_measure_data = network_measure_data.fillna(0)
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_ENTITY_COUNT,
        F.col(NetworkMeasures.DIRECT_ENTITY_COUNT)
        + F.col(NetworkMeasures.INDIRECT_ENTITY_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_ENTITY_WEIGHT,
        F.col(NetworkMeasures.DIRECT_ENTITY_WEIGHT)
        + F.col(NetworkMeasures.INDIRECT_ENTITY_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_FLAG_COUNT,
        F.col(NetworkMeasures.DIRECT_FLAG_COUNT)
        + F.col(NetworkMeasures.INDIRECT_FLAG_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_FLAG_WEIGHT,
        F.col(NetworkMeasures.DIRECT_FLAG_WEIGHT)
        + F.col(NetworkMeasures.INDIRECT_FLAG_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_FLAGGED_COUNT,
        F.col(NetworkMeasures.DIRECT_FLAGGED_COUNT)
        + F.col(NetworkMeasures.INDIRECT_FLAGGED_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_FLAGGED_WEIGHT,
        F.col(NetworkMeasures.DIRECT_FLAGGED_WEIGHT)
        + F.col(NetworkMeasures.INDIRECT_FLAGGED_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.RELATED_ENTITY_SCORE,
        F.col(NetworkMeasures.DIRECT_ENTITY_SCORE)
        + F.col(NetworkMeasures.INDIRECT_ENTITY_SCORE),
    )

    # get network measures
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_ENTITY_COUNT,
        F.lit(1) + F.col(NetworkMeasures.RELATED_ENTITY_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_ENTITY_WEIGHT,
        F.col(NetworkMeasures.TARGET_ENTITY_WEIGHT)
        + F.col(NetworkMeasures.RELATED_ENTITY_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_FLAG_COUNT,
        F.col(NetworkMeasures.TARGET_FLAG_COUNT)
        + F.col(NetworkMeasures.RELATED_FLAG_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_FLAG_WEIGHT,
        F.col(NetworkMeasures.TARGET_FLAG_WEIGHT)
        + F.col(NetworkMeasures.RELATED_FLAG_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_FLAGGED_COUNT,
        F.col("TARGET_FLAGGED_COUNT") + F.col(NetworkMeasures.RELATED_FLAGGED_COUNT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_FLAGGED_WEIGHT,
        F.col("TARGET_FLAGGED_WEIGHT") + F.col(NetworkMeasures.RELATED_FLAGGED_WEIGHT),
    )
    network_measure_data = network_measure_data.withColumn(
        NetworkMeasures.NETWORK_ENTITY_SCORE,
        F.col(NetworkMeasures.TARGET_ENTITY_SCORE)
        + F.col(NetworkMeasures.RELATED_ENTITY_SCORE),
    )
    network_measure_data = network_measure_data.drop(
        *["TARGET_FLAGGED_COUNT", "TARGET_FLAGGED_WEIGHT"]
    )

    # normalize measures using max scaler and rank scaler
    measure_cols = [column for column in NetworkMeasures.list()]
    for measure in configs.selected_measures:
        if measure.normalization == NormalizationTypes.BY_MAX_VALUE:
            logger.info(f"Normalizing by max scale: {measure.name}")
            scaled_output_col = f"{measure.name}_{NormalizationTypes.BY_MAX_VALUE}"
            network_measure_data = normalize_max_scale(
                df=network_measure_data,
                input_col=measure.name,
                output_col=scaled_output_col,
            )
            measure_cols.append(scaled_output_col)
        else:
            logger.info(f"Normalizing by rank scale: {measure.name}")
            scaled_output_col = f"{measure.name}_{NormalizationTypes.BY_RANK_VALUE}"
            network_measure_data = normalize_rank(
                df=network_measure_data,
                input_col=measure.name,
                output_col=scaled_output_col,
            )
            measure_cols.append(scaled_output_col)

    # collect all calculated measures to be used for scoring
    all_measures = F.create_map(
        *list(chain(*[[F.lit(name), F.col(name)] for name in measure_cols]))
    )
    network_measure_data = network_measure_data.withColumn("all_measures", all_measures)

    # calculate the final network score
    __network_score_udf = F.udf(
        lambda measure_values: compute_score(measure_values, configs), FloatType()
    )
    network_measure_data = network_measure_data.withColumn(
        ScoringTypes.FINAL_NETWORK_SCORE, __network_score_udf("all_measures")
    )
    logger.info(f"Finished computing raw final network score")

    # normalize the final network score
    scaled_score_col = (
        f"{ScoringTypes.FINAL_NETWORK_SCORE}_{NormalizationTypes.BY_MAX_VALUE}"
    )
    network_measure_data = normalize_max_scale(
        df=network_measure_data,
        input_col=ScoringTypes.FINAL_NETWORK_SCORE,
        output_col=scaled_score_col,
    )

    return network_measure_data


def __compute_related_entity_measures(
    entity_score_data: DataFrame,
    related_entity_data: DataFrame,
    measure_category: NetworkMeasureCategories = NetworkMeasureCategories.DIRECT,
) -> DataFrame:
    """
    Compute all network measures in a given measure category (e.g. directly-related entities or indirectly-related entities).

    Params:
    --------------
        entity_score_data: DataFrame
            Contains measures and score of individual entities
        related_entity_data: DataFrame
            Dataframe storing predicted link data, with columns [Source, Target]
        measure_category: NetworkMeasureCategories
            The category of measures to be calculated (directly-related or indirect-related entities)

    Returns:
    ---------------
        DataFrame: dataframe with the unnormalized network measures
    """
    # get entity measures for the source and target nodes
    entity_measures = [
        column for column in entity_score_data.columns if column != ENTITY_ID
    ]
    measure_data = related_entity_data.join(
        entity_score_data,
        related_entity_data[TARGET_NODE] == entity_score_data[ENTITY_ID],
        how="inner",
    ).drop(ENTITY_ID)
    for measure in entity_measures:
        measure_data = measure_data.withColumnRenamed(
            measure, f"{TARGET_NODE}_{measure}"
        )

    # calculate network measures for all related entities
    network_measure_data = measure_data.groupby(SOURCE_NODE).agg(
        F.sum(f"{TARGET_NODE}_{EntityMeasures.FLAG_COUNT}").alias(
            f"{measure_category}_{EntityMeasures.FLAG_COUNT}"
        ),
        F.sum(f"{TARGET_NODE}_{EntityMeasures.FLAG_WEIGHT}").alias(
            f"{measure_category}_{EntityMeasures.FLAG_WEIGHT}"
        ),
        F.sum(f"{TARGET_NODE}_{EntityMeasures.ENTITY_WEIGHT}").alias(
            f"{measure_category}_{EntityMeasures.ENTITY_WEIGHT}"
        ),
        F.sum(f"{TARGET_NODE}_{ScoringTypes.FINAL_ENTITY_SCORE}").alias(
            f"{measure_category}_{ScoringTypes.FINAL_ENTITY_SCORE}"
        ),
        F.countDistinct(TARGET_NODE).alias(f"{measure_category}_entity_count"),
    )

    # calculate network measures for related flagged entities
    flagged_entity_data = measure_data.filter(
        F.col(f"{TARGET_NODE}_{EntityMeasures.FLAG_COUNT}") > 0
    )
    flagged_entity_data = flagged_entity_data.groupby(SOURCE_NODE).agg(
        F.countDistinct(TARGET_NODE).alias(f"{measure_category}_flagged_count"),
        F.sum(f"{TARGET_NODE}_{EntityMeasures.ENTITY_WEIGHT}").alias(
            f"{measure_category}_flagged_weight"
        ),
    )

    # join all measures
    network_measure_data = network_measure_data.join(
        flagged_entity_data, on=SOURCE_NODE, how="left"
    )
    network_measure_data = network_measure_data.fillna(0)
    network_measure_data = network_measure_data.withColumnRenamed(
        SOURCE_NODE, ENTITY_ID
    )

    return network_measure_data


def __compute_flag_degree(  # nosec - B107
    entity_score_data: DataFrame,
    related_entity_data: DataFrame,
    attribute_join_token: str = "::",
) -> DataFrame:
    """
    Compute measures relate to review flag degree for each entity.
    Review flag degree = number of unique attributes in a source entity's flagged links.
    There are two types of flagged links:
    - Source/Target has review flags;
    - Source and Target are connected via a direct activity (dynamic) link.
    When calculating degree, only count attributes that are directly linked to a source entity.

    Params:
    ---------------
        entity_score_data: DataFrame
            Contains measures and score of individual entities
        related_entity_data: DataFrame
            Dataframe storing predicted link data, with columns [Source, Target, Paths]
        attribute_join_token: str, default = '::'
            Token used to join attribute::value pair in the paths data

    Returns:
    ---------------
        Dataframe: Dataframe with entity's flag degree and flag degree type measures
    """
    # get all flagged links
    flag_data = entity_score_data.select(ENTITY_ID, EntityMeasures.FLAG_COUNT)
    flagged_link_data = related_entity_data.join(
        flag_data, related_entity_data[SOURCE_NODE] == flag_data[ENTITY_ID], how="inner"
    ).drop(ENTITY_ID)
    flagged_link_data = flagged_link_data.withColumnRenamed(
        EntityMeasures.FLAG_COUNT, f"{SOURCE_NODE}_{EntityMeasures.FLAG_COUNT}"
    )
    flagged_link_data = flagged_link_data.join(
        flag_data, flagged_link_data[TARGET_NODE] == flag_data[ENTITY_ID], how="inner"
    ).drop(ENTITY_ID)
    flagged_link_data = flagged_link_data.withColumnRenamed(
        EntityMeasures.FLAG_COUNT, f"{TARGET_NODE}_{EntityMeasures.FLAG_COUNT}"
    )
    flagged_link_data = flagged_link_data.withColumn(
        "has_direct_dynamic_path",
        direct_dynamic_link_udf(F.col(PATHS), F.lit(attribute_join_token)),
    )
    flagged_link_data = flagged_link_data.filter(
        (F.col(f"{SOURCE_NODE}_{EntityMeasures.FLAG_COUNT}") > 0)
        | (F.col(f"{TARGET_NODE}_{EntityMeasures.FLAG_COUNT}") > 0)
        | (F.col("has_direct_dynamic_path") == 1)
    )
    flagged_link_data = flagged_link_data.selectExpr(
        f"{SOURCE_NODE} AS {ENTITY_ID}", PATHS
    )
    flagged_link_data = flagged_link_data.groupby(ENTITY_ID).agg(
        F.collect_list(PATHS).alias(PATHS)
    )

    # compute flag degree measures
    flagged_link_data = flagged_link_data.withColumn(
        NetworkMeasures.TARGET_FLAG_DEGREE,
        __flag_degree_udf(F.col(ENTITY_ID), F.col(PATHS), F.lit(attribute_join_token)),
    )
    flagged_link_data = flagged_link_data.withColumn(
        NetworkMeasures.TARGET_FLAG_DEGREE_TYPE,
        __flag_degree_type_udf(
            F.col(ENTITY_ID), F.col(PATHS), F.lit(attribute_join_token)
        ),
    )
    flagged_link_data = flagged_link_data.drop(PATHS).dropDuplicates()

    return flagged_link_data


@udf(returnType=IntegerType())
def direct_dynamic_link_udf(  # nosec - B107
    paths: List[List[str]], attribute_join_token: str = "::"
):
    """
    Helper UDF to check if there is a direct dynamic path connecting two entities.

    Params:
        paths: List[List[str]]
            List of paths between an entity-entity link

    Returns:
        int: 1 if there is a direct dynamic path

    """
    for path in paths:
        if len(paths) > 3:
            return 0
        else:
            linking_node_value = path[1].split(attribute_join_token)[0]
            if linking_node_value.startswith(
                SYNC_ACTIVITY_PREFIX
            ) or linking_node_value.startswith(ASYNC_ACTIVITY_PREFIX):
                return 1
    return 0


@udf(returnType=IntegerType())
def __flag_degree_udf(  # nosec - B107
    entity: str, paths: List[List[List[str]]], attribute_join_token: str = "::"
):
    """
    Helper UDF to compute flag degree for a given entity.
    Flag degree = Number of unique attributes that directly connect to the given entity.

    Params:
        entity: str
            Name of the source entity to compute the flag degree for
        paths: List[List[List[str]]]
            List of paths between an entity-entity link
    Returns:
        int: entity flag degree

    """
    attributes = set()
    flatten_paths = chain.from_iterable(paths)
    for path in flatten_paths:
        # entity will either be the first or last node of the path
        if path[0].split(attribute_join_token)[1] == entity and len(path) > 1:
            attributes.add(path[1])
        elif path[-1].split(attribute_join_token)[1] == entity and len(path) > 1:
            attributes.add(path[-2])
    return len(attributes)


@udf(returnType=IntegerType())
def __flag_degree_type_udf(  # nosec - B107
    entity: str, paths: List[List[List[str]]], attribute_join_token: str = "::"
):
    """
    Helper UDF to compute flag degree type for a given entity.
    Flag degree type = Number of unique types of attributes that directly connect to the given entity.

    Params:
        entity: str
            Name of the source entity to compute the flag degree type for
        paths: List[List[List[str]]]
            List of paths between an entity-entity link
        attribute_join_token: str, default = '::'
            Token used to join attribute::value pair in the paths data
    Returns:
        int: entity flag degree type

    """
    attribute_types = set()
    flatten_paths = chain.from_iterable(paths)
    for path in flatten_paths:
        # entity will either be at the first or last node of the path
        if path[0].split(attribute_join_token)[1] == entity and len(path) > 1:
            attribute_types.add(path[1].split(attribute_join_token)[0])
        elif path[-1].split(attribute_join_token)[1] == entity and len(path) > 1:
            attribute_types.add(path[-2].split(attribute_join_token)[0])
    return len(attribute_types)


@udf(returnType=IntegerType())
def direct_related_entity_udf(paths: List[List[str]]):
    """
    Helper UDF to check if there is a direct path (including direct path with fuzzy matching).

    Params:
        paths: List[List[str]]
            List of paths between an entity-entity link
    Returns:
        int: 1 if there is a direct path

    """
    for path in paths:
        if len(path) <= 4:
            return 1
    return 0
