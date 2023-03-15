#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


import pyspark.sql.functions as F

from pyspark.sql import DataFrame

import transparency_engine.pipeline.schemas as schemas

from transparency_engine.modules.similarity.similarity_score import fill_empty_array_udf


def aggregate_all_activities(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
):
    """
    Aggregate all activities for each entity in the predicted links dataset.

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

    Returns:
    -------
        aggregated_df: Spark DataFrame
            The aggregated dataset.
    """
    overall_activities = activity_data.select(
        [activity_entity, activity_attribute]
    ).dropDuplicates()
    overall_activities = overall_activities.groupby(activity_entity).agg(
        F.collect_set(activity_attribute).alias(schemas.ACTIVITIES)
    )

    # join the all-time activities data into the aggregated dataset
    aggregated_df = overall_activities.selectExpr(
        f"{activity_entity} as {source_entity}", schemas.ACTIVITIES
    ).join(predicted_links, on=source_entity, how="inner")
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}"
    )
    aggregated_df = aggregated_df.join(
        overall_activities.selectExpr(
            f"{activity_entity} as {target_entity}", schemas.ACTIVITIES
        ),
        on=target_entity,
        how="inner",
    )
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.TARGET}_all_{schemas.ACTIVITIES}"
    )
    return aggregated_df


def aggregate_all_activities_by_type(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_type: str = schemas.TARGET_TYPE,
):
    """
    Aggregate all activities for each entity in the predicted links dataset.

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
        aggregated_df: Spark DataFrame
            The aggregated dataset.
    """
    overall_activities = activity_data.select(
        [activity_entity, activity_attribute, activity_type]
    ).dropDuplicates()
    overall_activities = overall_activities.groupby(activity_entity, activity_type).agg(
        F.collect_set(activity_attribute).alias(schemas.ACTIVITIES)
    )

    # join the all-time activities data into the aggregated dataset
    aggregated_df = overall_activities.selectExpr(
        f"{activity_entity} as {source_entity}", activity_type, schemas.ACTIVITIES
    ).join(predicted_links, on=source_entity, how="inner")
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.SOURCE}_all_{schemas.ACTIVITIES}"
    )
    aggregated_df = aggregated_df.join(
        overall_activities.selectExpr(
            f"{activity_entity} as {target_entity}", activity_type, schemas.ACTIVITIES
        ),
        on=[target_entity, activity_type],
        how="inner",
    )
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.TARGET}_all_{schemas.ACTIVITIES}"
    )
    return aggregated_df


def aggregate_period_activities(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_time: str = schemas.TIME_PERIOD,
) -> DataFrame:
    """
    Aggregate acitivities per time period, and across all time periods,
    to be used for subsequence scoring calculation

    Params:
    -------
        predicted_links: Spark DataFrame
            The predicted links dataset.  in the format of [source, target], (e.g. [A, B])
        activity_data: Spark DataFrame
            The activity dataset.  in the format of [source, target, time] (e.g. [A, X, '2017-Q3'])

    Returns:
    -------
        aggregated_df: Spark DataFrame
            The aggregated dataset.  in the format of [source, target, time, source_period_activities, target_period_activities]

    """

    # aggregate activities per period
    all_periods = activity_data.select(activity_time).dropDuplicates()
    all_entities = activity_data.select(activity_entity).dropDuplicates()
    all_entities = all_entities.join(F.broadcast(all_periods))
    period_activities = activity_data.select(
        [activity_entity, activity_attribute, activity_time]
    ).dropDuplicates()
    period_activities = period_activities.groupBy([activity_entity, activity_time]).agg(
        F.collect_set(schemas.TARGET).alias(schemas.ACTIVITIES)
    )
    period_activities = period_activities.join(
        all_entities, on=[activity_entity, activity_time], how="full"
    )
    period_activities = period_activities.withColumn(
        schemas.ACTIVITIES, fill_empty_array_udf(F.col(schemas.ACTIVITIES))
    ).persist()

    # join the period activities data into the predicted links data
    aggregated_df = period_activities.selectExpr(
        f"{activity_entity} as {source_entity}", schemas.ACTIVITIES, activity_time
    ).join(predicted_links, on=source_entity, how="inner")
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}"
    ).persist()
    aggregated_df = aggregated_df.join(
        period_activities.selectExpr(
            f"{activity_entity} as {target_entity}", schemas.ACTIVITIES, activity_time
        ),
        on=[target_entity, activity_time],
        how="inner",
    )
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.TARGET}_period_{schemas.ACTIVITIES}"
    )
    return aggregated_df


def aggregate_period_activities_by_type(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_time: str = schemas.TIME_PERIOD,
    activity_type: str = schemas.TARGET_TYPE,
) -> DataFrame:
    """
    Aggregate acitivities per time period, and across all time periods,
    to be used for subsequence scoring calculation.

    Params:
    -------
        predicted_links: Spark DataFrame
            The predicted links dataset.  in the format of [source, target], (e.g. [A, B])
        activity_data: Spark DataFrame
            The activity dataset.  in the format of [source, target, time] (e.g. [A, X, '2017-Q3'])

    Returns:
    -------
        aggregated_df: Spark DataFrame
            The aggregated dataset.  in the format of [source, target, time, source_period_activities, target_period_activities]
    """

    # aggregate activities per period
    all_periods = activity_data.select(activity_time).dropDuplicates()
    all_entities = activity_data.select(activity_entity, activity_type).dropDuplicates()
    all_entities = all_entities.join(F.broadcast(all_periods))
    period_activities = activity_data.select(
        [activity_entity, activity_attribute, activity_time, activity_type]
    ).dropDuplicates()
    period_activities = period_activities.groupBy(
        [activity_entity, activity_time, activity_type]
    ).agg(F.collect_set(schemas.TARGET).alias(schemas.ACTIVITIES))
    period_activities = period_activities.join(
        all_entities, on=[activity_entity, activity_time, activity_type], how="full"
    )
    period_activities = period_activities.withColumn(
        schemas.ACTIVITIES, fill_empty_array_udf(F.col(schemas.ACTIVITIES))
    ).persist()

    # join the period activities data into the predicted links data
    aggregated_df = period_activities.selectExpr(
        f"{activity_entity} as {source_entity}",
        schemas.ACTIVITIES,
        activity_time,
        activity_type,
    ).join(predicted_links, on=source_entity, how="inner")
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.SOURCE}_period_{schemas.ACTIVITIES}"
    )
    aggregated_df = aggregated_df.join(
        period_activities.selectExpr(
            f"{activity_entity} as {target_entity}",
            schemas.ACTIVITIES,
            activity_time,
            activity_type,
        ),
        on=[target_entity, activity_time, activity_type],
        how="inner",
    )
    aggregated_df = aggregated_df.withColumnRenamed(
        schemas.ACTIVITIES, f"{schemas.TARGET}_period_{schemas.ACTIVITIES}"
    )
    return aggregated_df
