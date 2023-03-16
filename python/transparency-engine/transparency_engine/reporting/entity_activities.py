#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from functools import reduce
from typing import Dict, List, Tuple, Union

import pyspark.sql.functions as F

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
from transparency_engine.reporting.entity_flags import summarize_related_entity_flags


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
) -> Tuple[DataFrame, DataFrame]:
    """
    Returns data needed to populated the Sync Activity and Async Activity in the report.
    """

    dynamic_link_data = _get_dynamic_links(predicted_link_data)

    summary_data = _summarize_activities(entity_data, dynamic_link_data)

    if not isinstance(dynamic_relationship_data, List):
        activity_data = dynamic_relationship_data
    else:
        activity_data = reduce(DataFrame.unionAll, dynamic_relationship_data)

    # sync links
    sync_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 1
    ).select(schemas.SOURCE, schemas.TARGET, schemas.PATHS)
    if sync_link_data.count() > 0:
        sync_summary_data = _summary_related_entities(
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
        summary_data = summary_data.join(
            sync_summary_data, on=schemas.ENTITY_ID, how="left"
        )
    else:
        summary_data = summary_data.withColumn(report_schemas.SYNC_ACTIVITY, F.lit(""))

    async_link_data = dynamic_link_data.filter(
        F.col(schemas.DYNAMIC_LINK_TYPE) == 2
    ).select(schemas.SOURCE, schemas.TARGET, schemas.PATHS)
    if async_link_data.count() > 0:
        async_summary_data = _summary_related_entities(
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
        summary_data = summary_data.join(
            async_summary_data, on=schemas.ENTITY_ID, how="left"
        )
    else:
        summary_data = summary_data.withColumn(report_schemas.ASYNC_ACTIVITY, F.lit(""))

    entity_activity_data = activity_data.selectExpr(
        f'{schemas.SOURCE} AS {schemas.ENTITY_ID}',
        f'{schemas.TARGET_TYPE} AS {schemas.ATTRIBUTE_ID}',
        f'{schemas.TARGET} AS {schemas.VALUE}',
        f'{schemas.TIME_PERIOD} AS {schemas.TIME_PERIOD}',
    )
    return (summary_data, entity_activity_data)


def _summarize_activities(
    entity_data: DataFrame, dynamic_link_data: DataFrame
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
    summary_data = summary_data.withColumn(
        report_schemas.ACTIVITY_SUMMARY,
        F.to_json(
            F.struct(report_schemas.SYNC_LINK_COUNT, report_schemas.ASYNC_LINK_COUNT)
        ),
    )
    summary_data = summary_data.select(
        schemas.ENTITY_ID, report_schemas.ACTIVITY_SUMMARY
    )
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
) -> DataFrame:
    """
    For each entity, summarize related entities with sync and async links.
    """
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
    summary_data = summary_data.join(
        related_flag_summaries, on=[schemas.ENTITY_ID, report_schemas.RELATED_ENTITY], how="left"
    )

    # get activity scoring summary
    scoring_summaries = summarize_link_scores(
        predicted_links=dynamic_link_data,
        activity_data=activity_data,
        link_attributes=link_attributes,
    )

    scoring_summaries = get_link_score_summary(scoring_summaries, link_attributes)
    scoring_summaries = scoring_summaries.withColumnRenamed(schemas.SOURCE, schemas.ENTITY_ID)\
                                            .withColumnRenamed(schemas.TARGET, report_schemas.RELATED_ENTITY)

    summary_data = summary_data.join(
            F.broadcast(scoring_summaries), on=[schemas.ENTITY_ID, report_schemas.RELATED_ENTITY], how="left"
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

    return summary_data


def _get_dynamic_links(predicted_link_data: DataFrame) -> DataFrame:
    """
    Summarize number of sync and async links each entity has.

    Params:
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]

    Returns:
        DataFrame: contains count of sync and async links for each entity
    """
    # summarize network flag measures for the Flag Summary section in the report
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
