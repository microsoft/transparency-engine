#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from itertools import chain

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType

from transparency_engine.analysis.scoring.measures import (
    EntityMeasures,
    MeasureConfig,
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
    ENTITY_ID,
    ENTITY_WEIGHT,
    REVIEW_FLAG_ID,
    REVIEW_FLAG_WEIGHT,
)


logger = logging.getLogger(__name__)

DEFAULT_ENTITY_SCORE_CONFIGS: ScoringConfig = ScoringConfig(
    selected_measures=[
        MeasureConfig(
            EntityMeasures.FLAG_WEIGHT,
            weight=1.0,
            normalization=NormalizationTypes.BY_MAX_VALUE,
        ),
        MeasureConfig(
            EntityMeasures.ENTITY_WEIGHT,
            weight=1.0,
            normalization=NormalizationTypes.BY_MAX_VALUE,
        ),
    ],
    scoring_function=ArithmeticOperators.MULTIPLICATION,
)


def compute_entity_score(
    entity_data: DataFrame,
    entity_flag_data: DataFrame,
    flag_metadata: DataFrame,
    configs: ScoringConfig = DEFAULT_ENTITY_SCORE_CONFIGS,
) -> DataFrame:
    """
    Compute all entity measures as specified in the EntityMeasures enum,
    then combine these measures to compute a single entity score using the scoring function
    specified in the yScoringConfig object.

    Params:
    --------------
        entity_data: DataFrame
            Entity dataframe, containig columns [EntityID, EntityWeight]
        entity_flag_data: DataFrame
            Dataframe storing REVIEW flags for each entity, containg columns [EntityID, ReviewFlagId]
        flag_metadata: DataFrame
            Metadata of REVIEW flags, containing columns [ReviewFlagID, ReviewFlagWeight]
        configs: ScoringConfig
            Configuration of the entity scoring function

    Returns:
    ---------------
        DataFrame: dataframe with computed entity measures and score (raw and normalized)
    """
    # compute entity flag count and flag weight
    flag_data = entity_flag_data.join(
        flag_metadata, on=REVIEW_FLAG_ID, how="inner"
    ).dropDuplicates()
    flag_data = flag_data.groupby(ENTITY_ID).agg(
        F.countDistinct(REVIEW_FLAG_ID).alias(EntityMeasures.FLAG_COUNT),
        F.sum(REVIEW_FLAG_WEIGHT).alias(EntityMeasures.FLAG_WEIGHT),
    )

    # join with entity weight
    entity_measure_data = entity_data.selectExpr(
        ENTITY_ID, f"{ENTITY_WEIGHT} AS {EntityMeasures.ENTITY_WEIGHT}"
    )
    entity_measure_data = entity_measure_data.select(
        ENTITY_ID, EntityMeasures.ENTITY_WEIGHT
    ).join(flag_data, on=ENTITY_ID, how="left")
    entity_measure_data = entity_measure_data.fillna(0)

    # normalize measures using max scaler and rank scaler
    measure_cols = [
        column for column in entity_measure_data.columns if column != ENTITY_ID
    ]
    all_measure_cols = []
    for column in measure_cols:
        logger.info(f"Normalizing: {column}")
        max_scaled_output_col = f"{column}_{NormalizationTypes.BY_MAX_VALUE}"
        entity_measure_data = normalize_max_scale(
            df=entity_measure_data, input_col=column, output_col=max_scaled_output_col
        )
        all_measure_cols.append(column)
        all_measure_cols.append(max_scaled_output_col)

        rank_scaled_output_col = f"{column}_{NormalizationTypes.BY_RANK_VALUE}"
        entity_measure_data = normalize_rank(
            df=entity_measure_data,
            input_col=column,
            output_col=rank_scaled_output_col,
            remove_null=True,
            remove_zero=False,
            reset_zero=True,
        )
        all_measure_cols.append(rank_scaled_output_col)

    # collect all calculated measures to be used for scoring
    all_measures = F.create_map(
        *list(chain(*[[F.lit(name), F.col(name)] for name in all_measure_cols]))
    )
    entity_measure_data = entity_measure_data.withColumn("all_measures", all_measures)

    # calculate entity score
    __entity_score_udf = F.udf(
        lambda measure_values: compute_score(measure_values, configs), FloatType()
    )
    entity_measure_data = entity_measure_data.withColumn(
        ScoringTypes.FINAL_ENTITY_SCORE, __entity_score_udf("all_measures")
    )
    return entity_measure_data
