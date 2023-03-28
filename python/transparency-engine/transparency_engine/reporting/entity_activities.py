#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from functools import reduce
from typing import Dict, List, Tuple, Union

import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

import transparency_engine.pipeline.schemas as schemas
import transparency_engine.reporting.report_schemas as report_schemas

from transparency_engine.analysis.scoring.measures import NetworkMeasures
from transparency_engine.modules.graph.link_filtering.dynamic_link_scoring import (
    get_link_score_summary,
    summarize_link_scores,
)
from transparency_engine.modules.graph.link_filtering.period_scoring import (
    summarize_temporal_activities,
)
from transparency_engine.modules.stats.aggregation import percent_rank
from transparency_engine.reporting.entity_flags import summarize_related_entity_flags
from transparency_engine.spark.utils import spark


@dataclass
class ActivityReportOutput:
    entity_activity: DataFrame
    entity_activity_summary_scores: DataFrame
    entity_link_counts: DataFrame
    entity_link_all_scores: DataFrame
    entity_link_temporal_scores: DataFrame
    entity_link_overall_scores: DataFrame


def report_activities(  # nosec - B107
    entity_data: DataFrame,
    predicted_link_data: DataFrame,
    dynamic_relationship_data: Union[DataFrame, List[DataFrame]],
    sync_link_attributes: List[str],
    async_link_attributes: List[str],
    network_score_data: DataFrame,
    flag_summary_data: DataFrame,
    attribute_name_mapping: Dict[str, str],
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
) -> ActivityReportOutput:
    """
    Returns data needed to populated the Sync Activity and Async Activity sections in the report.

    Params:
    -------------
        entity_data: DataFrame
            Entity dataframe, containing columns EntityID, EntityWeight.
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]
        dynamic_relationship_data: Union[DataFrame, List[DataFrame]]
            List of all dynamic graph dataframes (from the multipartite graph edges)
        sync_attributes: List[str]
            List of attributes used to calculate synchronous activity scores
        async_attributes: List[str]
            List of attributes used to calculate asynchronous activity scores
        network_score_data: DataFrame
            Dataframe contains all network measures calculated in the scoring step
        flag_summary_data: DataFrame
            Contains entities' flag summaries
        attribute_name_mapping: Dict
            Mapping of AttributeID to name
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)

    Returns:
        ActivityReportOutput: contains 4 dataframes storing aggregated entity activitites and activity link scoring
    """

    dynamic_link_data = _get_dynamic_links(predicted_link_data)

    link_summary_data = _summarize_activity_links(entity_data, dynamic_link_data)
    link_all_scores = link_summary_data.select(
        schemas.ENTITY_ID, report_schemas.ACTIVITY_SUMMARY
    )
    
    if not isinstance(dynamic_relationship_data, List):
        activity_data = dynamic_relationship_data
    else:
        activity_data = reduce(DataFrame.unionAll, dynamic_relationship_data)

    # summarize counts of entity's activity details for each period
    entity_activity_count_data = summarize_temporal_activities(activity_data)
    
    # calculate sync and async link scores
    temporal_score_list = []
    overall_score_list = []
    sync_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 1
    ).select(schemas.SOURCE, schemas.TARGET, schemas.PATHS)
    if sync_link_data.count() > 0:
        sync_summary_data, sync_temporal_scores, sync_overall_scores = _summary_related_entities(
            dynamic_link_data=sync_link_data,
            activity_data=activity_data,
            link_attributes=sync_link_attributes,
            network_score_data=network_score_data,
            flag_summary_data=flag_summary_data,
            related_flag_col=report_schemas.SYNC_ACTIVITY,
            attribute_name_mapping=attribute_name_mapping,
            attribute_join_token=attribute_join_token,
            edge_join_token=edge_join_token,
        )
        link_all_scores = link_all_scores.join(
            sync_summary_data, on=schemas.ENTITY_ID, how="left"
        )
        temporal_score_list.append(sync_temporal_scores)
        overall_score_list.append(sync_overall_scores)
    else:
        link_all_scores = link_all_scores.withColumn(
            report_schemas.SYNC_ACTIVITY, F.lit("")
        )

    # async link scores
    async_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 2
    ).select(schemas.SOURCE, schemas.TARGET, schemas.PATHS)
    if async_link_data.count() > 0:
        async_summary_data, async_temporal_scores, async_overall_scores = _summary_related_entities(
            dynamic_link_data=async_link_data,
            activity_data=activity_data,
            link_attributes=async_link_attributes,
            network_score_data=network_score_data,
            flag_summary_data=flag_summary_data,
            related_flag_col=report_schemas.ASYNC_ACTIVITY,
            attribute_name_mapping=attribute_name_mapping,
            attribute_join_token=attribute_join_token,
            edge_join_token=edge_join_token,
        )
        link_all_scores = link_all_scores.join(
            async_summary_data, on=schemas.ENTITY_ID, how="left"
        )
        temporal_score_list.append(async_temporal_scores)
        overall_score_list.append(async_overall_scores)
    else:
        link_all_scores = link_all_scores.withColumn(
            report_schemas.ASYNC_ACTIVITY, F.lit("")
        )
    
    # aggregate temporal scores for activity links
    if len(temporal_score_list) > 0:
        entity_link_temporal_scores = reduce(DataFrame.unionAll, temporal_score_list)
    else:
        entity_link_temporal_scores = spark.createDataFrame(
            [], schemas.ENTITY_TEMPORAL_ACTIVITY_SCHEMA
        )

    # aggregate overall scores (jaccard score) for activity links
    if len(overall_score_list) > 0:
        entity_link_overall_scores = reduce(DataFrame.unionAll, overall_score_list)
    else:
        entity_link_overall_scores = spark.createDataFrame(
            [], schemas.ENTITY_OVERALL_ACTIVITY_SCHEMA
        )
   
    # reformat raw entity activity table
    entity_activity_data = activity_data.selectExpr(
        f"{schemas.SOURCE} AS {schemas.ENTITY_ID}",
        f"{schemas.TARGET_TYPE} AS {schemas.ATTRIBUTE_ID}",
        f"{schemas.TARGET} AS {schemas.VALUE}",
        f"{schemas.TIME_PERIOD} AS {schemas.TIME_PERIOD}",
    )

    activity_output = ActivityReportOutput(
        entity_activity=entity_activity_data,
        entity_activity_summary_scores=entity_activity_count_data,
        entity_link_counts=link_summary_data,
        entity_link_all_scores=link_all_scores,
        entity_link_temporal_scores=entity_link_temporal_scores,
        entity_link_overall_scores=entity_link_overall_scores,
    )
    return activity_output


def _summarize_activity_links(
    entity_data: DataFrame, dynamic_link_data: DataFrame, min_percent: float = 0.1
) -> DataFrame:
    """
    Summarize number of sync and async links each entity has.

    Params:
        entity_data: DataFrame
            Contains all entities to generate report for
        dynamic_link_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths, DynamicLinkType]

    Returns:
        DataFrame: contains count of sync and async links for each entity
    """
    sync_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 1
    ).withColumnRenamed(schemas.SOURCE, schemas.ENTITY_ID)
    sync_link_data = sync_link_data.groupby(schemas.ENTITY_ID).agg(
        F.countDistinct(schemas.TARGET).alias(report_schemas.SYNC_LINK_COUNT)
    )
    async_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 2
    ).withColumnRenamed(schemas.SOURCE, schemas.ENTITY_ID)
    async_link_data = async_link_data.groupby(schemas.ENTITY_ID).agg(
        F.countDistinct(schemas.TARGET).alias(report_schemas.ASYNC_LINK_COUNT)
    )
    summary_data = entity_data.join(sync_link_data, on=schemas.ENTITY_ID, how="left")
    summary_data = summary_data.join(async_link_data, on=schemas.ENTITY_ID, how="left")
    summary_data = summary_data.fillna(0)

    # compute percent rank for the counts
    measure_columns = [report_schemas.SYNC_LINK_COUNT, report_schemas.ASYNC_LINK_COUNT]
    all_measure_columns = []
    for column in measure_columns:
        output_col = f"{column}_{report_schemas.PERCENT_RANK_MEASURE_POSTFIX}"
        summary_data = percent_rank(
            df=summary_data,
            input_col=column,
            percent_col=output_col,
            min_percent=min_percent,
        )
        all_measure_columns.append(column)
        all_measure_columns.append(output_col)

    summary_data = summary_data.withColumn(
        report_schemas.ACTIVITY_SUMMARY,
        F.to_json(F.struct(*all_measure_columns)),
    )
    all_measure_columns.extend([schemas.ENTITY_ID, report_schemas.ACTIVITY_SUMMARY])
    summary_data = summary_data.select(all_measure_columns)
    return summary_data


def _summary_related_entities(  # nosec - B107
    dynamic_link_data: DataFrame,
    activity_data: DataFrame,
    link_attributes: List[str],
    network_score_data: DataFrame,
    flag_summary_data: DataFrame,
    related_flag_col: str,
    attribute_name_mapping: Dict[str, str],
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    For each entity, summarize related entities with sync and async links.

    Params:
        dynamic_link_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths, DynamicLinkType]
        activity_data: DataFrame
            Contains all actions of each entity
        link_attributes: List[str]
            List of attributes used to calculate the activity link scores
        network_score_data: DataFrame
            Dataframe contains all network measures calculated in the scoring step
        flag_summary_data: DataFrame
            Contains entities' flag summaries
        related_flag_col: str
            Name of the column indicating whether this is a sync or async activity
        attribute_name_mapping: Dict
            Mapping of AttributeID to name
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]: 3 dataframes containing overall and temporal activity scores

    """    
    # get link summary and related redflags,
    related_flag_summaries = summarize_related_entity_flags(
        flag_summary_data=flag_summary_data,
        predicted_link_data=dynamic_link_data,
        attribute_name_mapping=attribute_name_mapping,
        related_flag_col=related_flag_col,
        attribute_join_token=attribute_join_token,
        edge_join_token=edge_join_token,
        include_flagged_entities_only=False,
        group_related_entities=False,
    )
    related_flag_summaries = related_flag_summaries.select(
        schemas.ENTITY_ID,
        report_schemas.RELATED_ENTITY,
        report_schemas.LINK_SUMMARY,
        report_schemas.RELATED_FLAG_DETAILS,
    )
    
    # get activity scoring summary
    all_scores, temporal_scores = summarize_link_scores(
        predicted_links=dynamic_link_data,
        activity_data=activity_data,
        link_attributes=link_attributes,
    )
    scoring_summaries = get_link_score_summary(all_scores, link_attributes)
    scoring_summaries.show(5)
    scoring_summaries = scoring_summaries.withColumnRenamed(
        schemas.SOURCE, schemas.ENTITY_ID
    ).withColumnRenamed(schemas.TARGET, report_schemas.RELATED_ENTITY)
   
    # join all scores in one table
    summary_data = dynamic_link_data.withColumnRenamed(
        schemas.SOURCE, schemas.ENTITY_ID
    ).withColumnRenamed(schemas.TARGET, report_schemas.RELATED_ENTITY)
    summary_data = summary_data.join(
        network_score_data.selectExpr(
            f"{schemas.ENTITY_ID} AS {report_schemas.RELATED_ENTITY}",
            f"{NetworkMeasures.TARGET_FLAG_COUNT} AS {report_schemas.ACTIVITY_FLAG_COUNT}",
        ),
        on=report_schemas.RELATED_ENTITY,
        how="inner",
    )
    summary_data = (summary_data.join(
        related_flag_summaries,
        on=[schemas.ENTITY_ID, report_schemas.RELATED_ENTITY],
        how="left",
    )).join(
        scoring_summaries,
        on=[schemas.ENTITY_ID, report_schemas.RELATED_ENTITY],
        how="left",
    )

    report_columns = [
        report_schemas.RELATED_ENTITY,
        report_schemas.ACTIVITY_FLAG_COUNT,
        report_schemas.LINK_SUMMARY,
        report_schemas.AVERAGE_SCORE,
        report_schemas.PERIOD_SCORE,
    ]
    for attribute in link_attributes:
        report_columns.append(f"{attribute}_score")
    summary_data = summary_data.withColumn(
        related_flag_col, F.to_json(F.struct(*report_columns))
    )
    summary_data = summary_data.groupby(schemas.ENTITY_ID).agg(
        F.collect_list(related_flag_col).alias(related_flag_col)
    )
    
    # add link type to the all scores
    if related_flag_col == report_schemas.SYNC_ACTIVITY:
        all_scores = all_scores.withColumn(report_schemas.LINK_TYPE, F.lit(report_schemas.SYNC_ACTIVITY_LINK_TYPE))
    else:
        all_scores = all_scores.withColumn(report_schemas.LINK_TYPE, F.lit(report_schemas.ASYNC_ACTIVITY_LINK_TYPE))
    return summary_data, temporal_scores, all_scores


def _get_dynamic_links(predicted_link_data: DataFrame) -> DataFrame:
    """
    Get sync and async links from the predicted links.

    Params:
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]

    Returns:
        DataFrame: contains sync and async links
    """
    dynamic_link_data = predicted_link_data.withColumn(
        schemas.DYNAMIC_LINK_TYPE, dynamic_link_type_udf(schemas.PATHS)
    )
    dynamic_link_data = dynamic_link_data.filter(F.col(schemas.DYNAMIC_LINK_TYPE) > 0)
    return dynamic_link_data


@udf(returnType=IntegerType())
def dynamic_link_type_udf(  # nosec - B107
    paths: List[List[str]], attribute_join_token: str = "::"
):
    """
    Helper UDF to check if there is a synchronous activity path connecting two entities.

    Params:
        paths: List[List[str]]
            List of paths between an entity-entity link

    Returns:
        int: 1 if there is a direct synchronous path

    """
    for path in paths:
        if len(paths) > 3:
            return 0
        else:
            linking_node_value = path[1].split(attribute_join_token)[0]
            if linking_node_value.startswith(schemas.SYNC_ACTIVITY_PREFIX):
                return 1
            elif linking_node_value.startswith(schemas.ASYNC_ACTIVITY_PREFIX):
                return 2
    return 0
