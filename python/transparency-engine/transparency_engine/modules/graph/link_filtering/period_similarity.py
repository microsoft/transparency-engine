#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


import pyspark.sql.functions as F

from pyspark.sql import DataFrame

import transparency_engine.pipeline.schemas as schemas

from transparency_engine.modules.similarity.similarity_score import (
    multiple_set_intersection_size_udf,
    multiple_set_union_size_udf,
    set_intersection_udf,
)


def compute_period_similarity(
    aggregated_activity_data: DataFrame,
    source_entity: str = f"{schemas.SOURCE}",
    target_entity: str = f"{schemas.TARGET}",
    source_all_activities: str = f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}",
    target_all_activities: str = f"{schemas.TARGET}_all_{schemas.ACTIVITIES}",
    source_period_activities: str = f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
    target_period_activities: str = f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
):
    """
    Sync score = per-period intersections/all intersections
    invariant score = normalizer to penalize links that share the same type of activities across all periods
    Async score = 1 - sync score
    """
    period_scores = aggregated_activity_data.withColumn(
        "all_intersections",
        set_intersection_udf(source_all_activities, target_all_activities),
    )
    period_scores = period_scores.withColumn(
        "period_intersections",
        set_intersection_udf(source_period_activities, target_period_activities),
    )
    period_scores = period_scores.groupBy(
        source_entity, target_entity, "all_intersections"
    ).agg(F.collect_list("period_intersections").alias("period_intersections"))
    period_scores = period_scores.withColumn(
        "sync_intersection_size", multiple_set_union_size_udf("period_intersections")
    )
    period_scores = period_scores.withColumn(
        "sync_invariant_size",
        multiple_set_intersection_size_udf(F.col("period_intersections")),
    )
    period_scores = period_scores.withColumn(
        "sync_invariant_score",
        F.when(
            F.col("sync_intersection_size") > 0,
            1 - (F.col("sync_invariant_size") / F.col("sync_intersection_size")),
        ).otherwise(0),
    )

    # sync and async scores
    period_scores = period_scores.withColumn(
        "sync_similarity",
        F.col("sync_invariant_score")
        * F.col("sync_intersection_size")
        / F.size(F.col("all_intersections")),
    )
    period_scores = period_scores.withColumn(
        "async_similarity",
        F.col("sync_invariant_score")
        * (1 - (F.col("sync_intersection_size") / F.size(F.col("all_intersections")))),
    )
    period_scores = period_scores.drop(*["all_intersections", "period_intersections"])
    return period_scores
