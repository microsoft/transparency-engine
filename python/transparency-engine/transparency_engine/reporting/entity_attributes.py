#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from functools import reduce
from itertools import chain
from typing import Dict, List, Tuple, Union

import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame, Window

import transparency_engine.modules.data_shaper.spark_transform as spark_transform

from transparency_engine.pipeline.schemas import (
    ATTRIBUTE_ID,
    DESCRIPTION,
    ENTITY_ID,
    NAME,
    VALUE,
)
from transparency_engine.reporting.report_schemas import (
    ATTRIBUTE_SUMMARY,
    DEFAULT_ENTITY_NAME_ATTRIBUTE,
)


@dataclass
class AttributeReportOutput:
    entity_attribute_metadata: DataFrame
    entity_attributes: DataFrame
    entity_attribute_summary: DataFrame


def report_entity_attributes(
    entity_data: DataFrame,
    static_relationship_data: Union[DataFrame, List[DataFrame]],
    other_attribute_data: Union[DataFrame, List[DataFrame]],
    entity_name_attribute: str,
    default_entity_name_attribute: str = DEFAULT_ENTITY_NAME_ATTRIBUTE,
    attribute_metadata: Union[DataFrame, None] = None,
) -> AttributeReportOutput:
    """
    Generates 3 output tables to be used for entity search and for populating the
    Entity Overview section of the entity report:
    - Updated attribute metata: attribute definitions for all entity attributes.
    - Entity attribute data: Combine all attributes in the Entity, static relationship, and other covariate attribute tables into a single table
    - Attribute summary: Count for each attribute in the entity attribute table

    Params:
    -------------
        entity_data: DataFrame
            Entity dataframe, containing columns EntityID, EntityWeight.
        static_relationship_data: Union[DataFrame, List[DataFrame]]
            List of all static relationship dataframes.
        other_attribute_data: Union[DataFrame, List[DataFrame]]
            List of all other entity attributes that are not used for link prediction
            but can be included for search and reporting.
        entity_name_attribute: str
            Name of the static attribute used as the default entity name.
        default_entity_name_attribute: str, default = "default_name"
            Name of the default entity name attribute to be added in the output dataframe
        attribute_metadata: DataFrame, default = None
            Dataframe contaiing attribute definition, with schema [AttributeID, Name, Description].
            If None, a default metadata will be created.

    Returns:
    -------------
        AttributeReportOutput:
            Returns the updated attribute metadata, entity attributes, and attribute counts.
    """
    # aggregate all entity attributes
    all_attribute_metadata, all_entity_attribute_data = __get_entity_attributes(
        entity_data=entity_data,
        static_relationship_data=static_relationship_data,
        other_attribute_data=other_attribute_data,
        entity_name_attribute=entity_name_attribute,
        default_entity_name_attribute=default_entity_name_attribute,
        attribute_metadata=attribute_metadata,
    )

    attribute_summary_data = __summarize_entity_attributes(
        entity_data=entity_data,
        entity_attribute_data=all_entity_attribute_data,
        default_entity_name_attribute=default_entity_name_attribute,
    )

    output = AttributeReportOutput(
        entity_attribute_metadata=all_attribute_metadata,
        entity_attributes=all_entity_attribute_data,
        entity_attribute_summary=attribute_summary_data,
    )
    return output


def __get_entity_attributes(
    entity_data: DataFrame,
    static_relationship_data: Union[DataFrame, List[DataFrame]],
    other_attribute_data: Union[DataFrame, List[DataFrame]],
    entity_name_attribute: str,
    default_entity_name_attribute: str = DEFAULT_ENTITY_NAME_ATTRIBUTE,
    attribute_metadata: Union[DataFrame, None] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Combine all static attributes of entities into a single table, to be used
    for entity search, and for populating the attribute_values fields in the Entity Overview
    section of the entity report.

    Params:
    -------------
        entity_data: DataFrame
            Entity dataframe, containing columns EntityID, EntityWeight.
        static_relationship_data: Union[DataFrame, List[DataFrame]]
            List of all static relationship dataframes.
        other_attribute_data: Union[DataFrame, List[DataFrame]]
            List of other entity attributes that are not used for link prediction
            but can be included for search and reporting.
        entity_name_attribute: str
            Name of the static attribute used as the default entity name.
        default_entity_name_attribute: str, default = "default_name"
            Name of the default entity name attribute to be added in the output dataframe.
        attribute_metadata: DataFrame, default = None
            Dataframe contaiing attribute definition, with schema [AttributeID, Name, Description]

    Returns:
    -------------
        Tuple[DataFrame, DataFrame]:
            Returns the updated attribute metadata and a dataframe containing entity static attributes.
    """
    # aggregate all static attributes
    if not isinstance(static_relationship_data, List):
        static_relationship_data = [static_relationship_data]
    if not isinstance(other_attribute_data, List):
        other_attribute_data = [other_attribute_data]

    melted_entity_data = spark_transform.melt(
        data=entity_data, id_cols=ENTITY_ID, attribute_col=ATTRIBUTE_ID, value_col=VALUE
    )
    attribute_data_list: List[DataFrame] = list(
        chain.from_iterable(
            [static_relationship_data, other_attribute_data, [melted_entity_data]]
        )
    )
    all_attribute_data = reduce(DataFrame.unionAll, attribute_data_list)

    # add default entity name attribute
    all_attribute_data = __add_default_entity_name(
        entity_attribute_data=all_attribute_data,
        entity_name_attribute=entity_name_attribute,
        default_entity_name_attribute=default_entity_name_attribute,
    )

    # update attribute metadata
    if attribute_metadata is None:
        all_attribute_metadata = all_attribute_data.select(
            ATTRIBUTE_ID
        ).dropDuplicates()
        all_attribute_metadata = all_attribute_metadata.withColumn(
            NAME, F.col(ATTRIBUTE_ID)
        ).withColumn(DESCRIPTION, F.lit(""))
    else:
        # add the attributes from the Entity table into the metadata if needed
        missing_metadata = (
            all_attribute_data.select(ATTRIBUTE_ID)
            .dropDuplicates()
            .join(attribute_metadata, on=ATTRIBUTE_ID, how="leftanti")
        )
        if missing_metadata.count() > 0:
            missing_metadata = missing_metadata.withColumn(
                NAME, F.col(ATTRIBUTE_ID)
            ).withColumn(DESCRIPTION, F.lit(""))
            all_attribute_metadata = attribute_metadata.union(missing_metadata)
        else:
            all_attribute_metadata = attribute_metadata
    all_attribute_metadata = all_attribute_metadata.withColumn(
        NAME,
        F.when(
            (F.col(NAME).isNull()) | (F.trim(F.col(NAME)) == ""), F.col(ATTRIBUTE_ID)
        ).otherwise(F.col(NAME)),
    )

    # join the updated attribute metadata with the attribute table
    all_attribute_data = all_attribute_data.join(
        all_attribute_metadata, on=ATTRIBUTE_ID, how="left"
    )
    return (all_attribute_metadata, all_attribute_data)


def __add_default_entity_name(
    entity_attribute_data: DataFrame,
    entity_name_attribute: str,
    default_entity_name_attribute: str = DEFAULT_ENTITY_NAME_ATTRIBUTE,
) -> DataFrame:
    """
    Add a default entity name attribute to the entity attribute table
    to enable the frontend to display a single entity name in the entity search/summary table.

    Params:
        entity_attribute_data: DataFrame
            Dataframe containing all entity attributes collected from the static relationship tables and the Entity table
        entity_name_attribute: str
            Name of the static attribute used as the default entity name.
        default_entity_name_attribute: str
            Name of the default entity name attribute to be added in the output dataframe.

    Return:
        DataFrame: Updated entity attribute table with the added default name attribute.
    """
    entity_name_data = (
        entity_attribute_data.filter(F.col(ATTRIBUTE_ID) == entity_name_attribute)
        .drop(entity_name_attribute)
        .cache()
    )
    if entity_name_data.count() > 0:
        window_spec = Window.partitionBy(ENTITY_ID).orderBy(VALUE)
        entity_name_data = entity_name_data.withColumn(
            "name_rank", F.row_number().over(window_spec)
        )
        entity_name_data = entity_name_data.filter(F.col("name_rank") == 1)
        entity_name_data = entity_name_data.withColumn(
            ATTRIBUTE_ID, F.lit(default_entity_name_attribute)
        )
        entity_name_data = entity_name_data.select(ENTITY_ID, ATTRIBUTE_ID, VALUE)
        updated_attribute_dta = entity_attribute_data.union(entity_name_data)
        return updated_attribute_dta
    else:
        return entity_attribute_data


def __summarize_entity_attributes(
    entity_data: DataFrame,
    entity_attribute_data: DataFrame,
    default_entity_name_attribute: str = DEFAULT_ENTITY_NAME_ATTRIBUTE,
) -> DataFrame:
    """
    Compute count for each attribute in the entity attribute table,
    except for attributes in the entity_data table (because these attributes always have count = 1)

    Params:
    -------------
        entity_data: DataFrame
            Entity dataframe, containing columns EntityID, EntityWeight. This dataframe
            may also contain other attributes that are not used for entity relationship linking,
            but can be included in the final entity report
        entity_attribute_data: DataFrame
            Dataframe containing aggregated entity attributes collected from the static relationship tables and the Entity table
        default_entity_name_attribute: str
            Name of the default entity name attribute in the aggregated entity attribute table.
        attribute_metadata: DataFrame
            Dataframe contaiing attribute definition, with schema [AttributeID, Name, Description]

    Returns:
    -------------
        DataFrame: Dataframe with counts of all static attributes
    """
    # compute count of values for all static attributes
    excluded_attributes = [
        column for column in entity_data.columns if column != ENTITY_ID
    ]
    excluded_attributes.append(default_entity_name_attribute)
    summary_data = entity_attribute_data.groupby([ENTITY_ID, ATTRIBUTE_ID, NAME]).agg(
        F.countDistinct(VALUE).alias("count")
    )
    summary_data = summary_data.filter(~F.col(ATTRIBUTE_ID).isin(excluded_attributes))
    summary_data = summary_data.groupby(ENTITY_ID).pivot(NAME).sum("count")

    # add all counts to json format
    count_columns = [column for column in summary_data.columns if column != ENTITY_ID]
    summary_data = summary_data.withColumn(
        ATTRIBUTE_SUMMARY, F.to_json(F.struct(*count_columns))
    )
    summary_data = summary_data.select(ENTITY_ID, ATTRIBUTE_SUMMARY)
    return summary_data


def get_attribute_mapping(attribute_metadata: DataFrame) -> Dict[str, str]:
    """
    Get mapping of {AttributeID:Name}.

    Params:
        attribute_metadata: DataFrame
            Definitions of entity attributes

    Returns:
        Dict[str, str] AttributeID:Name mapping
    """
    attribute_mapping = {}
    for row in attribute_metadata.collect():
        attribute_mapping[str(row[ATTRIBUTE_ID])] = row[NAME]
    return attribute_mapping
