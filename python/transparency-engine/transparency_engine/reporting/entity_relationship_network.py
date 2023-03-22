#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from enum import Enum
from typing import Dict, List

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

from transparency_engine.analysis.scoring.measures import NetworkMeasures
from transparency_engine.analysis.scoring.network_scoring import direct_dynamic_link_udf
from transparency_engine.pipeline.schemas import (
    ENTITY_ID,
    PATHS,
    SOURCE_NODE,
    TARGET_NODE,
)
from transparency_engine.reporting.report_schemas import (
    PATH_SOURCE_FLAG,
    PATH_SOURCE_NODE,
    PATH_SOURCE_RELATIONSHIP,
    PATH_SOURCE_TYPE,
    PATH_TARGET_FLAG,
    PATH_TARGET_NODE,
    PATH_TARGET_RELATIONSHIP,
    PATH_TARGET_TYPE,
)


logger = logging.getLogger(__name__)


class NodeRelationshipTypes(str, Enum):
    """
    Enum for types of relationship between nodes in an entity network
    """

    TARGET_ENTITY = "target"
    RELATED_ENTITY = "related"
    ENTITY_ATTRIBUTE = "attribute"

    @staticmethod
    def from_string(relationship_type: str):
        """
        Converts a string to a relationshp type enum.

        Params
        ----------
        relationship_type : str
            The string to convert.

        Returns
        -------
        NodeRelationshipTypes The NodeRelationshipTypes enum.
        """
        if relationship_type == "target":
            return NodeRelationshipTypes.TARGET_ENTITY
        elif relationship_type == "related_entity":
            return NodeRelationshipTypes.RELATED_ENTITY
        elif relationship_type == "attribute":
            return NodeRelationshipTypes.ENTITY_ATTRIBUTE
        else:
            raise ValueError(f"Unsupported relationship type: {relationship_type}")


def report_entity_graph(  # nosec - B107
    predicted_link_data: DataFrame,
    network_score_data: DataFrame,
    attribute_name_mapping: Dict[str, str],
    include_flagged_links_only: bool = False,
    attribute_join_token: str = "::",
):
    """
    Create edge list data required to build a network of closely-linked entities for each entity.

    Params:
    ------------
        predicted_links_data: DataFrame
            Contains predicted node links, with schema [Source, Target, Paths]
        network_score_data: DataFrame
            Contains entity scores related to review flags
        attribute_name_mapping: Dict[str, str]
            Mappings of AttributeID::Name
        include_flagged_links_only: boolean, default = False
            If True, only generate networks for nodes that either have a flag in their network
            or have a direct activity paths
        attribute_join_token: str, default = '::'
            String token used to join the attribute::value nodes in the Paths column of the predicted links table

    Returns:
        DataFrame: Graph edge data used to build the entity graph in the final report

    """
    # add missing mirrored links
    mirrored_link_data = predicted_link_data.selectExpr(
        f"{TARGET_NODE} AS {SOURCE_NODE}", f"{SOURCE_NODE} AS {TARGET_NODE}", PATHS
    )
    mirrored_link_data = mirrored_link_data.join(
        predicted_link_data, on=[SOURCE_NODE, TARGET_NODE], how="leftanti"
    )
    graph_data = predicted_link_data.union(mirrored_link_data).cache()
    logger.info(f"Number of links: {graph_data.count()}")

    if include_flagged_links_only:
        # only retain links if there is a flag in the entity network, or if there is a direct activity path connecting the node-node links
        graph_data = graph_data.join(
            network_score_data.selectExpr(
                f"{ENTITY_ID} AS {SOURCE_NODE}", NetworkMeasures.NETWORK_FLAG_COUNT
            ),
            on=SOURCE_NODE,
            how="inner",
        )
        graph_data = graph_data.withColumn(
            "has_activity_paths",
            direct_dynamic_link_udf(F.col(PATHS), F.lit(attribute_join_token)),
        )
        graph_data = graph_data.filter(
            (F.col(NetworkMeasures.NETWORK_FLAG_COUNT) > 0)
            | (F.col("has_activity_paths") == 1)
        )
        graph_data = graph_data.select(SOURCE_NODE, TARGET_NODE, PATHS).cache()
        logger.info(f"Number of filtered links: {graph_data.count()}")

    # generate graph edge data
    graph_data = graph_data.select(
        SOURCE_NODE, TARGET_NODE, F.explode(PATHS).alias("path")
    )

    graph_data = graph_data.withColumn("path_edges", get_path_edges_udf(F.col("path")))
    graph_data = graph_data.select(
        SOURCE_NODE, TARGET_NODE, F.explode("path_edges").alias("path_edge")
    )
    graph_data = graph_data.withColumn(
        PATH_SOURCE_NODE, F.col("path_edge").getItem(0)
    ).withColumn(PATH_TARGET_NODE, F.col("path_edge").getItem(1))
    graph_data = graph_data.withColumn(
        PATH_SOURCE_TYPE,
        F.split(F.col(PATH_SOURCE_NODE), attribute_join_token).getItem(0),
    ).withColumn(
        PATH_SOURCE_NODE,
        F.split(F.col(PATH_SOURCE_NODE), attribute_join_token).getItem(1),
    )
    graph_data = graph_data.withColumn(
        PATH_TARGET_TYPE,
        F.split(F.col(PATH_TARGET_NODE), attribute_join_token).getItem(0),
    ).withColumn(
        PATH_TARGET_NODE,
        F.split(F.col(PATH_TARGET_NODE), attribute_join_token).getItem(1),
    )
    graph_data = graph_data.drop("path_edge").dropDuplicates()

    # edge flag data for node in the entity graph
    graph_data = graph_data.join(
        network_score_data.selectExpr(
            f"{ENTITY_ID} AS {PATH_SOURCE_NODE}",
            f"{NetworkMeasures.TARGET_FLAG_COUNT} AS {PATH_SOURCE_FLAG}",
        ),
        on=PATH_SOURCE_NODE,
        how="left",
    )
    graph_data = graph_data.join(
        network_score_data.selectExpr(
            f"{ENTITY_ID} AS {PATH_TARGET_NODE}",
            f"{NetworkMeasures.TARGET_FLAG_COUNT} AS {PATH_TARGET_FLAG}",
        ),
        on=PATH_TARGET_NODE,
        how="left",
    )
    graph_data = graph_data.fillna(0)
    graph_data = graph_data.withColumn(
        PATH_SOURCE_FLAG, F.when(F.col(PATH_SOURCE_FLAG) > 0, 1).otherwise(0)
    ).withColumn(PATH_TARGET_FLAG, F.when(F.col(PATH_TARGET_FLAG) > 0, 1).otherwise(0))
    # map node type column to attribute name rather than attribute id
    __type_mapping_udf = F.udf(
        lambda type: attribute_name_mapping.get(type, type), StringType()
    )
    graph_data = graph_data.withColumn(
        PATH_SOURCE_TYPE, __type_mapping_udf(PATH_SOURCE_TYPE)
    )
    graph_data = graph_data.withColumn(
        PATH_TARGET_TYPE, __type_mapping_udf(PATH_TARGET_TYPE)
    )

    # add relationship indicator for path nodes
    graph_data = graph_data.withColumn(
        PATH_SOURCE_RELATIONSHIP,
        get_node_relationship_udf(SOURCE_NODE, PATH_SOURCE_NODE, PATH_SOURCE_TYPE),
    )
    graph_data = graph_data.withColumn(
        PATH_TARGET_RELATIONSHIP,
        get_node_relationship_udf(SOURCE_NODE, PATH_TARGET_NODE, PATH_TARGET_TYPE),
    )

    return graph_data


@udf(returnType=ArrayType(ArrayType(StringType())))
def get_path_edges_udf(path: List[str]) -> List[List]:
    edges = []
    for index in range(len(path) - 1):
        edges.append([path[index], path[index + 1]])
    return edges


@udf(returnType=StringType())
def get_node_relationship_udf(target_node: str, path_node: str, path_node_type: str):
    if path_node == target_node:
        return NodeRelationshipTypes.TARGET_ENTITY.value
    elif path_node_type == ENTITY_ID:
        return NodeRelationshipTypes.RELATED_ENTITY.value
    else:
        return NodeRelationshipTypes.ENTITY_ATTRIBUTE.value
