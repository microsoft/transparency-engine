#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

import transparency_engine.pipeline.schemas as schemas

from transparency_engine.modules.similarity.similarity_score import (
    set_intersection_size_udf,
    set_intersection_udf,
)


def compute_normalizer(
    aggregated_activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    source_all_activities: str = f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}",
    target_all_activities: str = f"{schemas.TARGET}_all_{schemas.ACTIVITIES}",
    source_period_activities: str = f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
    target_period_activities: str = f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
    activity_time: str = "time",
):
    """
    Compute a normalizer factor to penalize links with small number of overlapping periods
    """
    normalizer = aggregated_activity_data.withColumn(
        "all_intersections",
        set_intersection_udf(source_all_activities, target_all_activities),
    )
    normalizer = normalizer.withColumn(
        f"{schemas.SOURCE}_intersection_size",
        set_intersection_size_udf(source_period_activities, "all_intersections"),
    )
    normalizer = normalizer.withColumn(
        f"{schemas.TARGET}_intersection_size",
        set_intersection_size_udf(target_period_activities, "all_intersections"),
    )
    normalizer = normalizer.withColumn(
        "intersection_exist",
        F.when(
            (F.col(f"{schemas.SOURCE}_intersection_size") > 0)
            | (F.col(f"{schemas.TARGET}_intersection_size") > 0),
            1,
        ).otherwise(0),
    )
    normalizer = normalizer.groupBy(source_entity, target_entity).agg(
        F.sum("intersection_exist").alias("intersection_periods")
    )
    period_count = aggregated_activity_data.select(activity_time).distinct().count()
    normalizer = normalizer.withColumn(
        "normalizer", F.col("intersection_periods") / F.lit(period_count)
    )
    return normalizer
