#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List, Union

from dataclasses import dataclass
from pyspark.sql import DataFrame

from transparency_engine.pipeline.schemas import ENTITY_ID
from transparency_engine.reporting.entity_activities import report_activities
from transparency_engine.reporting.entity_attributes import (
    get_attribute_mapping,
    report_entity_attributes,
)
from transparency_engine.reporting.entity_flags import report_flags
from transparency_engine.reporting.entity_relationship_network import (
    report_entity_graph,
)

logger = logging.getLogger(__name__)

@dataclass
class ReportConfig:
    """
    Configuration for the final entity report.

    Params:
        include_flagged_links_only: bool, default = False
            If True, we only generate entity graphs for entities that either have a flag in their network
            or have a direct activity paths
        min_percent: float, default = 0.1
            Round up percent rank value to this min percentile for network flag measures
    """

    sync_link_attributes: List[str]
    async_link_attributes: List[str]
    include_flagged_links_only: bool = False
    min_percent: float = 0.1


@dataclass
class ReportOutput:
    entity_attributes: DataFrame
    entity_activity: DataFrame
    entity_graph: DataFrame
    html_report: DataFrame


def generate_report(  # nosec - B107
    entity_data: DataFrame,
    static_relationship_data: Union[DataFrame, List[DataFrame]],
    dynamic_graph_data: Union[DataFrame, List[DataFrame]],
    other_attribute_data: Union[DataFrame, List[DataFrame]],
    entity_flag_data: DataFrame,
    network_score_data: DataFrame,
    predicted_link_data: DataFrame,
    flag_metadata: DataFrame,
    configs: ReportConfig,
    attribute_metadata: Union[DataFrame, None] = None,
    attribute_join_token: str = "::",
    edge_join_token: str = "--",
) -> ReportOutput:
    """
    Generate 4 tables needed to populate the final entity report:
    - Entity attribute data: Combine all attributes in the Entity, static relationship, and other covariate attribute tables into a single table
    - Entity activity data: All entity dynamic attributes in a single table
    - Entity graph: graph of the target entity and its closely-related entities
    - Entity HTML report: summary data needed to populate the entity's HTML report

    Params:
    -------------
        entity_data: DataFrame
            Entity dataframe, containing columns EntityID, EntityWeight.
        static_relationship_data: Union[DataFrame, List[DataFrame]]
            List of all static relationship dataframes.
        dynamic_graph_data: Union[DataFrame, List[DataFrame]]
            List of all dynamic graph dataframes (from the multipartite graph edges)
        other_attribute_data: Union[DataFrame, List[DataFrame]]
            List of all other entity attributes that are not used for link prediction
            but can be included for search and reporting.
        network_score_data: DataFrame
            Dataframe contains all network measures calculated in the scoring step
        entity_flag_data: DataFrame
            Contains entities' flag details
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]
        flag_metadata: DataFrame, default = None
            Contains review flag definition with schema [FlagID, Category, Description, IsSupportingFlag, FlagWeight]
        attribute_metadata: DataFrame, default = None
            Dataframe contaiing attribute definition, with schema [AttributeID, Name, Description].
            If None, a default metadata will be created
        configs: ReportConfig
            Configuration of the entity report
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table
        edge_join_token: str, default = "--"
            String token used to join entity pairs with dynamic activity links (e.g. EntityA--EntityB)

    Returns:
        ReportOutput: Returns entity attributes, entity_activity, entity graph, html report data

    """
    # generate entity attributes table
    (
        updated_attribute_metadata,
        entity_attribute_data,
        attribute_summary_data,
    ) = report_entity_attributes(
        entity_data=entity_data,
        static_relationship_data=static_relationship_data,
        other_attribute_data=other_attribute_data,
        attribute_metadata=attribute_metadata,
    )
    logger.info('Finished generating entity attribute report')

    # generate entity graph
    attribute_name_mapping = get_attribute_mapping(updated_attribute_metadata)
    entity_graph_data = report_entity_graph(
        predicted_link_data=predicted_link_data,
        network_score_data=network_score_data,
        attribute_name_mapping=attribute_name_mapping,
        include_flagged_links_only=configs.include_flagged_links_only,
        attribute_join_token=attribute_join_token,
    ).cache()
    logger.info(f'Finished generating entity graph report: {entity_graph_data.count()}')

    # generate flag data
    flag_data = report_flags(
        entity_flag_data=entity_flag_data,
        network_score_data=network_score_data,
        predicted_link_data=predicted_link_data,
        flag_metadata=flag_metadata,
        attribute_name_mapping=attribute_name_mapping,
        min_percent=configs.min_percent,
        attribute_join_token=attribute_join_token,
        edge_join_token=edge_join_token,
    ).cache()
    logger.info(f'Finished generating entity flag report: {flag_data.count()}')

    # generate activity data
    activity_summary_data, entity_activity_data = report_activities(
        entity_data=entity_data,
        predicted_link_data=predicted_link_data,
        dynamic_relationship_data=dynamic_graph_data,
        sync_link_attributes=configs.sync_link_attributes,
        async_link_attributes=configs.async_link_attributes,
        network_score_data=network_score_data,
        flag_summary_data=flag_data,
        attribute_name_mapping=attribute_name_mapping,
        attribute_join_token=attribute_join_token,
        edge_join_token=edge_join_token,
    )
    logger.info('Finished generating entity acitity report')

    # generate html report data
    html_report_data = attribute_summary_data.join(flag_data, on=ENTITY_ID, how="left")
    html_report_data = html_report_data.join(
        activity_summary_data, on=ENTITY_ID, how="left"
    ).cache()
    logger.info(f'Finished generating html report: {html_report_data.count()}')

    report_output = ReportOutput(
        entity_attributes=entity_attribute_data,
        entity_activity=entity_activity_data,
        entity_graph=entity_graph_data,
        html_report=html_report_data,
    )
    return report_output
