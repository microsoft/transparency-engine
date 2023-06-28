#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from functools import reduce
from typing import Union

from pyspark.sql import DataFrame, functions as F

import transparency_engine.pipeline.schemas as schemas

import transparency_engine.modules.data_shaper.spark_transform as transform
from transparency_engine.modules.graph.link_filtering.activity_scoring import (
    aggregate_all_activities,
    aggregate_all_activities_by_type,
    aggregate_period_activities,
    aggregate_period_activities_by_type,
)
from transparency_engine.modules.similarity.similarity_score import (
    jaccard_similarity_udf,
    overlap_coefficient_udf,
    set_difference_udf,
    set_intersection_udf,
)


logger = logging.getLogger(__name__)


def compute_overall_overlap_score(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_type: Union[str, None] = None,
) -> DataFrame:
    """
    Calculate overall jaccard score of overlapping activities
    between source and target entities across all time periods.

    Params:
    -------
        predicted_links: Spark DataFrame
            The predicted links dataset.
        activity_data: Spark DataFrame
            The activity dataset.
        source_entity: str
            The name of the source entity column.
        target_entity: str
            The name of the target entity column.
        activity_entity: str
            The name of the activity entity column.
        activity_attribute: str
            The name of the activity attribute column.
        activity_type: str
            The name of the activity type column.

    Returns:
    -------
        all_activities_df: Spark DataFrame
            The dataset with overall jaccard score of overlapping activities.
    """
    if activity_type is None:
        all_activities_df = aggregate_all_activities(
            predicted_links=predicted_links,
            activity_data=activity_data,
            source_entity=source_entity,
            target_entity=target_entity,
            activity_entity=activity_entity,
            activity_attribute=activity_attribute,
        )
    else:
        all_activities_df = aggregate_all_activities_by_type(
            predicted_links=predicted_links,
            activity_data=activity_data,
            source_entity=source_entity,
            target_entity=target_entity,
            activity_entity=activity_entity,
            activity_attribute=activity_attribute,
            activity_type=activity_type,
        )
    all_activities_df = all_activities_df.withColumn(
        f"shared_{schemas.ACTIVITIES}",
        set_intersection_udf(
            f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_all_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        schemas.SHARED, F.size(f"shared_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        f"{schemas.SOURCE}_only_{schemas.ACTIVITIES}",
        set_difference_udf(
            f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}", f"shared_{schemas.ACTIVITIES}"
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        f"{schemas.SOURCE}_only", F.size(f"{schemas.SOURCE}_only_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        f"{schemas.TARGET}_only_{schemas.ACTIVITIES}",
        set_difference_udf(
            f"{schemas.TARGET}_all_{schemas.ACTIVITIES}", f"shared_{schemas.ACTIVITIES}"
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        f"{schemas.TARGET}_only", F.size(f"{schemas.TARGET}_only_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        schemas.JACCARD_SIMILARITY,
        jaccard_similarity_udf(
            f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_all_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        schemas.OVERLAP_SCORE,
        overlap_coefficient_udf(
            f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_all_{schemas.ACTIVITIES}",
        ),
    )
    return all_activities_df


def compute_all_overall_overlap_score(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_type: str = schemas.TARGET_TYPE,
) -> DataFrame:
    """
    Compute overall overlap scores for both all activity types, and for each activity type separately

    Params:
    -------
        predicted_links: Spark DataFrame
            The predicted links dataset.
        activity_data: Spark DataFrame
            The activity dataset.
        source_entity: str
            The name of the source entity column.
        target_entity: str
            The name of the target entity column.
        activity_entity: str
            The name of the activity entity column.
        activity_attribute: str
            The name of the activity attribute column.
        activity_type: str
            The name of the activity type column.

    Returns:
    -------
        overall_scores_df: Spark DataFrame
            The dataset with overall jaccard score of overlapping activities.
            In the format of [source, target, source_only, target_only, shared, jaccard_similarity, overlap_score]

    """
    overall_scores_df = compute_overall_overlap_score(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_type=None,
    ).cache()
    logger.info(f"overall score count: {overall_scores_df.count()}")

    overall_scores_by_type_df = compute_overall_overlap_score(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_type=activity_type,
    ).cache()
    logger.info(f"overall score by type count: {overall_scores_by_type_df.count()}")
    overall_scores_by_type_df.show(5)

    overall_scores_df = overall_scores_df.selectExpr(
        f"{source_entity} as {schemas.SOURCE}",
        f"{target_entity} as {schemas.TARGET}",
        '"overall" as type',
        schemas.SHARED,
        f"{schemas.SOURCE}_only",
        f"{schemas.TARGET}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )

    overall_scores_by_type_df = overall_scores_by_type_df.selectExpr(
        f"{source_entity} as {schemas.SOURCE}",
        f"{target_entity} as {schemas.TARGET}",
        f"{activity_type} as type",
        schemas.SHARED,
        f"{schemas.SOURCE}_only",
        f"{schemas.TARGET}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )
    overall_scores_df = overall_scores_df.union(overall_scores_by_type_df).cache()
    logger.info(f"total score record count: {overall_scores_df.count()}")

    # mirror the scores
    mirrored_overall_scores_df = overall_scores_df.selectExpr(
        f" {schemas.TARGET} as {schemas.SOURCE}",
        f"{schemas.SOURCE} as {schemas.TARGET}",
        "type",
        schemas.SHARED,
        f"{schemas.SOURCE}_only as {schemas.TARGET}_only",
        f"{schemas.TARGET}_only as {schemas.SOURCE}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )

    overall_scores_df = overall_scores_df.union(mirrored_overall_scores_df).cache()
    logger.info(f"overall score record count: {overall_scores_df.count()}")
    return overall_scores_df


def compute_period_overlap_score(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_time: str = schemas.TIME_PERIOD,
    activity_type: Union[str, None] = None,
) -> DataFrame:
    """
    Calculate overall jaccard score of overlapping activities
    between source and target entities across all time periods

    Params:
    --------
        predicted_links: Spark DataFrame
            A dataframe containing source and target entities
        activity_data: Spark DataFrame
            A dataframe containing activity data
        source_entity: str
            Name of the source entity column
        target_entity: str
            Name of the target entity column
        activity_entity: str
            Name of the activity entity column
        activity_attribute: str
            Name of the activity attribute column
        activity_time: str
            Name of the activity time column
        activity_type: str
            Name of the activity type column

    Returns:
    --------
        Spark DataFrame
    """
    if activity_type is None:
        all_activities_df = aggregate_period_activities(
            predicted_links=predicted_links,
            activity_data=activity_data,
            source_entity=source_entity,
            target_entity=target_entity,
            activity_entity=activity_entity,
            activity_attribute=activity_attribute,
            activity_time=activity_time,
        )

    else:
        all_activities_df = aggregate_period_activities_by_type(
            predicted_links=predicted_links,
            activity_data=activity_data,
            source_entity=source_entity,
            target_entity=target_entity,
            activity_entity=activity_entity,
            activity_attribute=activity_attribute,
            activity_time=activity_time,
        )

    all_activities_df = all_activities_df.withColumn(
        f"shared_{schemas.ACTIVITIES}",
        set_intersection_udf(
            f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        schemas.SHARED, F.size(f"shared_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        f"{schemas.SOURCE}_only_{schemas.ACTIVITIES}",
        set_difference_udf(
            f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
            f"shared_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        f"{schemas.SOURCE}_only", F.size(f"{schemas.SOURCE}_only_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        f"{schemas.TARGET}_only_{schemas.ACTIVITIES}",
        set_difference_udf(
            f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
            f"shared_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        f"{schemas.TARGET}_only", F.size(f"{schemas.TARGET}_only_{schemas.ACTIVITIES}")
    )

    all_activities_df = all_activities_df.withColumn(
        schemas.JACCARD_SIMILARITY,
        jaccard_similarity_udf(
            f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
        ),
    )
    all_activities_df = all_activities_df.withColumn(
        schemas.OVERLAP_SCORE,
        overlap_coefficient_udf(
            f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}",
            f"{schemas.TARGET}_period_{schemas.ACTIVITIES}",
        ),
    )
    return all_activities_df


def compute_all_period_overlap_score(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_type: str = schemas.TARGET_TYPE,
    activity_time: str = schemas.TIME_PERIOD,
) -> DataFrame:
    """
    Compute period overlap scores for both all activity types, and for each activity type separately\
    
    Params:
    --------
        predicted_links: Spark DataFrame
            A dataframe containing source and target entities
        activity_data: Spark DataFrame
            A dataframe containing activity data
        source_entity: str
            Name of the source entity column
        target_entity: str
            Name of the target entity column
        activity_entity: str
            Name of the activity entity column
        activity_attribute: str
            Name of the activity attribute column
        activity_time: str
            Name of the activity time column.
        activity_type: str
            Name of the activity type column.

    Returns:
    --------
        scores_df: DataFrames
            Overall scores [source, target, type, period, shared, source_only, target_only, jaccard_similarity, overlap_score]
    """
    scores_df = compute_period_overlap_score(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_time=activity_time,
        activity_type=None,
    )

    scores_by_type_df = compute_period_overlap_score(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_time=activity_time,
        activity_type=activity_type,
    )

    scores_df = scores_df.selectExpr(
        f"{source_entity} as {schemas.SOURCE}",
        f"{target_entity} as {schemas.TARGET}",
        '"overall" as type',
        f"{activity_time} as {schemas.TIME_PERIOD}",
        schemas.SHARED,
        f"{schemas.SOURCE}_only",
        f"{schemas.TARGET}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )

    scores_by_type_df = scores_by_type_df.selectExpr(
        f"{source_entity} as {schemas.SOURCE}",
        f"{target_entity} as {schemas.TARGET}",
        f"{activity_type} as type",
        f"{activity_time} as {schemas.TIME_PERIOD}",
        schemas.SHARED,
        f"{schemas.SOURCE}_only",
        f"{schemas.TARGET}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )

    scores_df = scores_df.union(scores_by_type_df).cache()

    mirrored_scores_df = scores_df.selectExpr(
        f"{schemas.TARGET} as {schemas.SOURCE}",
        f"{schemas.SOURCE} as {schemas.TARGET}",
        "type",
        schemas.TIME_PERIOD,
        schemas.SHARED,
        f"{schemas.TARGET}_only as {schemas.SOURCE}_only",
        f"{schemas.SOURCE}_only as {schemas.TARGET}_only",
        schemas.JACCARD_SIMILARITY,
        schemas.OVERLAP_SCORE,
    )
    scores_df = scores_df.union(mirrored_scores_df)
    return scores_df


def compute_active_period_summary_by_type(
    temporal_overlap_scores: DataFrame, activity_type: str
) -> DataFrame:
    """
    Compute shared/source_only/target_only periods

    Params:
    -------
        temporal_overlap_scores: Spark DataFrame
            Temporal overlap scores computed from the compute_all_period_overlap_score() method. Input data schema [source, target, type, period, shared, source_only, target_only]
        activity_type: str
            Activity type to compute the active period summary

    Returns:
    --------
        Spark DataFrame
            Active period summary for the given activity type. Output data schema [source, target, type, source_periods, target_periods, shared_periods]
    """
    overlap_scores = temporal_overlap_scores.filter(F.col("type") == activity_type)

    # calculate source active periods
    source_periods = overlap_scores.filter(
        (F.col(schemas.SHARED) > 0) | (F.col(f"{schemas.SOURCE}_only") > 0)
    )
    source_periods = source_periods.groupby(
        [f"{schemas.SOURCE}", f"{schemas.TARGET}"]
    ).agg(F.count(schemas.TIME_PERIOD).alias(f"{schemas.SOURCE}_periods"))

    # calculate target active periods
    target_periods = overlap_scores.filter(
        (F.col(schemas.SHARED) > 0) | (F.col(f"{schemas.TARGET}_only") > 0)
    )
    target_periods = target_periods.groupby(
        [f"{schemas.SOURCE}", f"{schemas.TARGET}"]
    ).agg(F.count(schemas.TIME_PERIOD).alias(f"{schemas.TARGET}_periods"))

    # calculate shared active periods
    shared_periods = overlap_scores.filter(F.col(schemas.SHARED) > 0)
    shared_periods = shared_periods.groupby(
        [f"{schemas.SOURCE}", f"{schemas.TARGET}"]
    ).agg(F.count(schemas.TIME_PERIOD).alias("shared_periods"))

    period_scores = (
        overlap_scores.select([f"{schemas.SOURCE}", f"{schemas.TARGET}"])
        .dropDuplicates()
        .join(source_periods, on=[f"{schemas.SOURCE}", f"{schemas.TARGET}"], how="left")
        .join(target_periods, on=[f"{schemas.SOURCE}", f"{schemas.TARGET}"], how="left")
        .join(shared_periods, on=[f"{schemas.SOURCE}", f"{schemas.TARGET}"], how="left")
    )
    period_scores = period_scores.fillna(0)

    # calculate overlap scores
    overlap_score_udf = F.udf(
        lambda shared, source, target: shared / min(source + shared, target + shared)
    )
    period_scores = period_scores.withColumn(
        "period_jaccard_score",
        F.col("shared_periods")
        / (
            F.col("shared_periods")
            + F.col(f"{schemas.SOURCE}_periods")
            + F.col(f"{schemas.TARGET}_periods")
        ),
    )
    period_scores = period_scores.withColumn(
        "period_overlap_score",
        overlap_score_udf(
            "shared_periods", f"{schemas.SOURCE}_periods", f"{schemas.TARGET}_periods"
        ),
    )
    return period_scores


def compute_all_active_period_summary(temporal_overlap_scores: DataFrame) -> DataFrame:
    """
    Compute shared/source_only/target_only periods for all activity types

    Params:
    -------
        temporal_overlap_scores: Spark DataFrame
            Temporal overlap scores computed from the compute_all_period_overlap_score() method. Input data schema [source, target, type, period, shared, source_only, target_only]

    Returns:
    --------
        Spark DataFrame
            Active period summary for all activity types. Output data schema [source, target, type, source_periods, target_periods, shared_periods]
    """
    activity_types = [
        row[0] for row in temporal_overlap_scores.select("type").distinct().collect()
    ]
    logger.info(f"activity types: {activity_types}")
    all_score_list = []
    for activity_type in activity_types:
        logger.info(f"calculating activity summary for {activity_type}")
        period_scores = compute_active_period_summary_by_type(
            temporal_overlap_scores, activity_type
        )
        period_scores = period_scores.withColumn("type", F.lit(activity_type))
        all_score_list.append(period_scores)
    all_scores = reduce(DataFrame.unionAll, all_score_list)
    return all_scores


def summarize_temporal_activities(
    activity_data: DataFrame,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_attribute_type: str = schemas.TARGET_TYPE,
    activity_time: str = schemas.TIME_PERIOD,
) -> DataFrame:
    """
    Compute count of activity details for each time period.

    Params:
        activity_data: Spark DataFrame
            Dataframe containing the raw activity attributes, with schema [Source, Target, SourceType, TargetType, TimePeriod]
        activity_entity: str
            Name of the entity column in the activity dataframe
        activity_attribute: str
            Name of the activity attribute column in the activity dataframe
        activity_time: str
            Name of the activity time period column in the activity_dataframe

    Returns:
        Spark DataFrame: contains count for each activity attribute per period
    """
    result_list = []
    # calculate overall activity detail counts for each period
    all_periods = activity_data.select(activity_time).dropDuplicates()
    all_entities = activity_data.select(activity_entity).dropDuplicates()
    all_entities = all_entities.join(F.broadcast(all_periods))

    overall_activity_counts = activity_data.groupby(
        [activity_entity, activity_time]
    ).agg(F.countDistinct(activity_attribute).alias("activity_count"))
    overall_activity_counts = all_entities.join(
        overall_activity_counts, on=[activity_entity, activity_time], how="left"
    )
    overall_activity_counts = overall_activity_counts.fillna(0)
    overall_activity_counts = overall_activity_counts.withColumn(
        "type", F.lit("overall")
    )
    result_list.append(overall_activity_counts)

    # calculate activity detail count by type for each period
    attribute_types = transform.column_to_list(activity_data, activity_attribute_type)
    for type in attribute_types:
        activity_type_data = activity_data.filter(
            F.col(activity_attribute_type) == type
        )
        activity_type_counts = activity_type_data.groupby(
            [activity_entity, activity_time]
        ).agg(F.countDistinct(activity_attribute).alias("activity_count"))
        activity_type_counts = all_entities.join(
            activity_type_counts, on=[activity_entity, activity_time], how="left"
        )
        activity_type_counts = activity_type_counts.fillna(0)
        activity_type_counts = activity_type_counts.withColumn("type", F.lit(type))
        result_list.append(activity_type_counts)
    summary_scores = reduce(DataFrame.unionAll, result_list)
    return summary_scores
