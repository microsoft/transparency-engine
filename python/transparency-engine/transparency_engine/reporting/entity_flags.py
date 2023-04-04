#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import Dict, List

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, StringType

from transparency_engine.analysis.scoring.network_scoring import (
    direct_related_entity_udf,
)
from transparency_engine.modules.stats.aggregation import percent_rank
from transparency_engine.pipeline.schemas import (
    DESCRIPTION,
    ENTITY_ID,
    EVIDENCE,
    PATHS,
    REVIEW_FLAG_ID,
    SOURCE_NODE,
    TARGET_NODE,
)
from transparency_engine.reporting.report_schemas import (
    DIRECT_FLAGS,
    DIRECT_LINK_COUNT,
    DIRECT_LINKS,
    FLAG_DESCRIPTION,
    FLAG_EVIDENCE,
    FLAG_SUMMARY_MEASURES,
    INDIRECT_FLAGS,
    INDIRECT_LINK_COUNT,
    LINK_SUMMARY,
    OWN_FLAGS,
    PERCENT_RANK_MEASURE_POSTFIX,
    RELATED_ENTITY,
    RELATED_FLAG_DETAILS,
    REVIEW_FLAG_SUMMARY,
)


logger = logging.getLogger(__name__)

"""
This module contains functions that produce data required to populate sections
related to review flags in the entity report.
"""


def report_flags(  # nosec - B107
    entity_flag_data: DataFrame,
    network_score_data: DataFrame,
    predicted_link_data: DataFrame,
    flag_metadata: DataFrame,
    attribute_name_mapping: Dict[str, str],
    min_percent: float = 0.1,
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
) -> DataFrame:
    """
    Report data on network flag summary, target entity's own flags and related entities' flags.

    Params:
        network_score_data: DataFrame
            Dataframe contains all network measures calculated in the scoring step
        entity_flag_data: DataFrame
            Contains entities' flag details
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]
        flag_metadata: DataFrame, default = None
            Contains review flag definition with schema [FlagID, Category, Description, IsSupportingFlag, FlagWeight]
        attribute_name_mapping: Dict[str, str]
            Mappings of AttributeID::Name
        min_percent: float, default = 0.1
            Round up percent rank value to this min percentile
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)
    """
    # summarize network flag measures for the Flag Summary section in the report
    summary_data = __summarize_flag_measures(
        network_score_data=network_score_data, min_percent=min_percent
    )

    # summarize entity's own flags for the Own Flags section in the report
    own_flags_data = __summarize_entity_flags(
        entity_flag_data=entity_flag_data, flag_metadata=flag_metadata
    )

    # summarize directly-related entities' flag details for the Directly-related Entities section
    related_entity_data = predicted_link_data.withColumn(
        "is_direct", direct_related_entity_udf(PATHS)
    )
    direct_flag_data = related_entity_data.filter(F.col("is_direct") == 1)
    direct_flag_data = summarize_related_entity_flags(
        flag_summary_data=own_flags_data,
        predicted_link_data=direct_flag_data,
        attribute_name_mapping=attribute_name_mapping,
        related_flag_col=DIRECT_FLAGS,
        attribute_join_token=attribute_join_token,
        edge_join_token=edge_join_token,
    )

    # summarize directly-related entities' flag details for the Directly-related Entities section
    indirect_flag_data = related_entity_data.filter(F.col("is_direct") == 0)
    indirect_flag_data = summarize_related_entity_flags(
        flag_summary_data=own_flags_data,
        predicted_link_data=indirect_flag_data,
        attribute_name_mapping=attribute_name_mapping,
        related_flag_col=INDIRECT_FLAGS,
        attribute_join_token=attribute_join_token,
        edge_join_token=edge_join_token,
    )

    summary_data = summary_data.join(own_flags_data, on=ENTITY_ID, how="left")
    summary_data = summary_data.join(direct_flag_data, on=ENTITY_ID, how="left")
    summary_data = summary_data.join(indirect_flag_data, on=ENTITY_ID, how="left")
    return summary_data


def __summarize_flag_measures(
    network_score_data: DataFrame, min_percent: float = 0.1
) -> DataFrame:
    """
    Combine a list of network measures into a single json format to populate the
    Review Flag Summary section in the report.

    Params:
    -------------
        network_score_data: DataFrame
            Dataframe contains all network measures calculated in the scoring step
        min_percent: float, default = 0.1
            Round up percent rank value to this min percentile
        percent_rank_tag: str, default = 'pct'
            Postfix used to tag a percentile rank column of a given measure

    Returns:
    -------------
        DataFrame: Dataframe with schema [EntityID, FlagSummary]
    """
    summary_data = network_score_data.select(ENTITY_ID, *FLAG_SUMMARY_MEASURES)

    # calculate percent rank for each measure
    measure_columns = [measure.value for measure in FLAG_SUMMARY_MEASURES]
    all_measure_columns = []
    for column in measure_columns:
        logger.info(f"Normalizing: {column}")
        output_col = f"{column}_{PERCENT_RANK_MEASURE_POSTFIX}"
        summary_data = percent_rank(
            df=summary_data,
            input_col=column,
            percent_col=output_col,
            min_percent=min_percent,
        )
        all_measure_columns.append(column)
        all_measure_columns.append(output_col)

    summary_data = summary_data.withColumn(
        REVIEW_FLAG_SUMMARY, F.to_json(F.struct(*all_measure_columns))
    )
    summary_data = summary_data.select(ENTITY_ID, REVIEW_FLAG_SUMMARY)
    return summary_data


def __summarize_entity_flags(
    entity_flag_data: DataFrame, flag_metadata: DataFrame
) -> DataFrame:
    """
    Combine all flags associated with an entity.

    Params:
    -------------
        entity_flag_data: DataFrame
            Contains entities' flag details
        flag_metadata: DataFrame, default = None
            Contains review flag definition with schema [FlagID, Category, Description, IsSupportingFlag, FlagWeight]

    Returns:
    -------------
        DataFrame: Dataframe with schema [EntityID, OwnFlags]
    """
    summary_data = entity_flag_data.join(flag_metadata, on=REVIEW_FLAG_ID, how="inner")
    summary_data = summary_data.withColumnRenamed(
        DESCRIPTION, FLAG_DESCRIPTION
    ).withColumnRenamed(EVIDENCE, FLAG_EVIDENCE)
    summary_data = summary_data.groupby([ENTITY_ID, FLAG_DESCRIPTION]).agg(
        F.collect_set(FLAG_EVIDENCE).alias(FLAG_EVIDENCE)
    )
    summary_data = summary_data.withColumn(
        OWN_FLAGS, F.to_json(F.struct(FLAG_DESCRIPTION, FLAG_EVIDENCE))
    )
    summary_data = summary_data.groupby(ENTITY_ID).agg(
        F.collect_list(OWN_FLAGS).alias(OWN_FLAGS)
    )
    return summary_data


def summarize_related_entity_flags(  # nosec - B107
    flag_summary_data: DataFrame,
    predicted_link_data: DataFrame,
    attribute_name_mapping: Dict[str, str],
    related_flag_col: str,
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
    include_flagged_entities_only: bool = True,
    group_related_entities: bool = True,
) -> DataFrame:
    """
    Generate summaries of related entities (direct or indirect), including
    summaries of shared attribute details and relate entity's flag details.

    Params:
    ------------
        flag_summary_data: DataFrame
            Summary of review flags for each entity, with schema [EntityID, OwnFlags]
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]
        attribute_name_mapping: Dict[str, str]
            Mappings of AttributeID::Name
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)
        include_flagged_entities_only: bool, default = True
            If True, only include related flaggted entities
        group_related_entities: bool, default = True
            If True, collect all related entities into a single column in the output data

    Returns:
    -------------
        DataFrame: Dataframe with schema [EntityID, RelatedFlags]
    """
    # get related entity's flag details
    if include_flagged_entities_only:
        summary_data = predicted_link_data.join(
            flag_summary_data,
            predicted_link_data[TARGET_NODE] == flag_summary_data[ENTITY_ID],
            how="inner",
        ).drop(ENTITY_ID)
    else:
        summary_data = predicted_link_data.join(
            flag_summary_data,
            predicted_link_data[TARGET_NODE] == flag_summary_data[ENTITY_ID],
            how="left",
        ).drop(ENTITY_ID)
    summary_data = summary_data.withColumnRenamed(OWN_FLAGS, RELATED_FLAG_DETAILS)

    # get link summary
    __direct_link_summary_udf = F.udf(
        lambda source, paths: __direct_link_summary(
            source, paths, attribute_name_mapping, attribute_join_token, edge_join_token
        ),
        ArrayType(StringType()),
    )
    summary_data = summary_data.withColumn(
        DIRECT_LINKS, __direct_link_summary_udf(SOURCE_NODE, PATHS)
    )
    summary_data = summary_data.withColumn(
        DIRECT_LINK_COUNT, __count_direct_links(PATHS)
    )
    summary_data = summary_data.withColumn(
        INDIRECT_LINK_COUNT, __count_indirect_links(PATHS)
    )
    summary_data = summary_data.withColumn(
        LINK_SUMMARY,
        F.to_json(F.struct(DIRECT_LINKS, DIRECT_LINK_COUNT, INDIRECT_LINK_COUNT)),
    )
    summary_data = summary_data.withColumnRenamed(
        SOURCE_NODE, ENTITY_ID
    ).withColumnRenamed(TARGET_NODE, RELATED_ENTITY)
    summary_data = summary_data.withColumn(
        related_flag_col,
        F.to_json(F.struct(RELATED_ENTITY, LINK_SUMMARY, RELATED_FLAG_DETAILS)),
    )

    if group_related_entities:
        summary_data = summary_data.groupby(ENTITY_ID).agg(
            F.collect_list(related_flag_col).alias(related_flag_col)
        )
    return summary_data


def __direct_link_summary(  # nosec - B107
    source: str,
    paths: List[List[str]],
    attribute_name_mapping: Dict[str, str],
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
) -> List[str]:
    """
    Generate a list of shared details for direct links,
    e.g.: [Phone number: 123456, Email: abc@gmail.com, Address: abc (similar address)].

    Params:
        source: str
            Source node
        paths: List[str]
            List of paths connecting two nodes
        attribute_name_mapping: Dict[str, str]
            Mappings of AttributeID::Name
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)
    """
    summaries = []
    for path in paths:
        if len(path) <= 3:
            # this is an absolute direct match, extract the connecting node as shared detail
            shared_detail = path[1].split(attribute_join_token)
            if edge_join_token not in shared_detail[1]:
                shared_detail_text = f"{attribute_name_mapping.get(shared_detail[0], shared_detail[0])}: {shared_detail[1]}"
            else:
                shared_detail_text = (
                    f"{attribute_name_mapping.get(shared_detail[0], shared_detail[0])}"
                )
            summaries.append(shared_detail_text)
        elif len(path) == 4:
            # this is a direct link with fuzzy match, get fuzzy-matched details
            if source != path[0]:
                path = list(reversed(path))
            shared_detail_source = path[1].split(attribute_join_token)
            shared_detail_target = path[2].split(attribute_join_token)
            shared_detail_text = f"{attribute_name_mapping.get(shared_detail_source[0], shared_detail_source[0])}: {shared_detail_source[1]} {edge_join_token} {shared_detail_target[1]}"
            summaries.append(shared_detail_text)
    return summaries


@udf(returnType=IntegerType())
def __count_direct_links(paths: List[List[str]]):
    return len([path for path in paths if len(path) <= 4])


@udf(returnType=IntegerType())
def __count_indirect_links(paths: List[List[str]]):
    return len([path for path in paths if len(path) > 4])
