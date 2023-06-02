#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import Union

import pyspark.sql.functions as F

from pyspark.sql import DataFrame, SparkSession

import transparency_engine.modules.similarity.similarity_score as similarity_score
import transparency_engine.pipeline.schemas as schemas

logger = logging.getLogger(__name__)

def generate_unipartite_edges(
    entity_attribute_data: DataFrame,
    fuzzy_match_data: Union[DataFrame, None],
    attribute: str,
    entity_col: str = "EntityID",
    attribute_col: str = "AttributeID",
    value_col: str = "Value",
    exact_match_weight: float = 1.0,
    use_jaccard_weight: bool = True,
) -> DataFrame:
    """
    Convert entity attribute data into edge list of an entity-entity unitpartite graph
    based on exact and fuzzy matching on a given attribute

    Parameters
    ----------
        entity_attribute_data: Spark DataFrame
            Contains data of all attributes with schema [entity_col, attribute_col, value_col]
        fuzzy_match_data: Spark DataFrame
            Contain results of fuzzy matching on attribute values, with schema [Source, Target, Similarity, attribute_col]
        attribute : str
            Name of the entity attribute to peform exact matching
        entity_col: str, default = 'EntityID'
            Name of the entity column in the entity_attribute_data dataframe
        attribute_col: str, default = 'AttributeID'
            Name of the attribute column in the entity_attribute_data dataframe
        value_col: str, default = 'Value'
            Name of the value column in the entity_attribute_data dataframe
        exact_match_weight: float, default = 1.0
            Default weight for an exact matching edge
        use_jaccard_weight: bool, default = True
            Whether to use jaccard similarity as edge weights

    Returns
    -------
        edge_data: Spark DataFrame
            The edge list of an entity-entity unipartite undirected graph with schema [Source, Target, SourceType, TargetType, Weight, Relationship]
    """
    # get edge data
    if use_jaccard_weight:
        edge_data = __generate_unipartite_jaccard_edges(
            entity_attribute_data=entity_attribute_data,
            fuzzy_match_data=fuzzy_match_data,
            attribute=attribute,
            entity_col=entity_col,
            attribute_col=attribute_col,
            value_col=value_col,
        )
    else:
        edge_data = __generate_unipartite_unnormalized_edges(
            entity_attribute_data=entity_attribute_data,
            fuzzy_match_data=fuzzy_match_data,
            attribute=attribute,
            entity_col=entity_col,
            attribute_col=attribute_col,
            value_col=value_col,
            exact_match_weight=exact_match_weight,
        )

    # mirror edges, then take half (source < target)
    mirrored_edge_data = edge_data.selectExpr(
        "Target AS Source", "Source AS Target", "Weight"
    )
    edge_data = edge_data.union(mirrored_edge_data)
    edge_data = edge_data.filter(F.col("Source") < F.col("Target")).dropDuplicates()

    # take max weight for each edge
    edge_data = edge_data.groupby(["Source", "Target"]).agg(
        F.max("Weight").alias("Weight")
    )

    edge_data = (
        edge_data.withColumn("SourceType", F.lit(entity_col))
        .withColumn("TargetType", F.lit(entity_col))
        .withColumn("Relationship", F.lit(attribute))
    )
    return edge_data


def generate_bipartite_edges(
    entity_attribute_data: DataFrame,
    fuzzy_match_data: Union[DataFrame, None],
    attribute: str,
    entity_col: str = "EntityID",
    attribute_col: str = "AttributeID",
    value_col: str = "Value",
    time_col: str = "TimePeriod",
    weight: float = 1.0,
) -> DataFrame:
    """
    Convert attribute data into an edge list of a bipartite graph

    Parameters
    ----------
        entity_attribute_data: Spark DataFrame
            Contains data of all attributes with schema [entity_col, attribute_col, value_col] or [entity_col, attribute_col, value_col, time_col]
        fuzzy_match_data: Spark DataFrame
            Contain results of fuzzy matching on attribute values, with schema [Source, Target, Similarity, attribute_col]
        attribute: str
            Name of the entity attribute to peform exact matching
        entity_col: str, default = 'EntityID'
            Name of the entity column in the entity_attribute_data dataframe
        attribute_col: str, default = 'AttributeID'
            Name of the attribute column in the entity_attribute_data dataframe
        value_col: str, default = 'Value'
            Name of the value column in the entity_attribute_data dataframe
        time_col: str, default = 'TimePeriod'
            Name of the time column in the entity_attribute_data dataframe
        weight: float, default = 1
            Default weight for an edge

    Returns
    -------
        edge_data: Spark DataFrame
            The edge list of a bipartite graph with schema [Source, Target, SourceType, TargetType, Weight, (time_col)]
    """
    edge_data = (
        entity_attribute_data.filter(F.col(attribute_col) == attribute)
        .withColumnRenamed(entity_col, "Source")
        .withColumnRenamed(value_col, "Target")
        .withColumn("SourceType", F.lit(entity_col))
        .withColumn("TargetType", F.lit(attribute))
        .withColumn("Weight", F.lit(weight))
        .dropDuplicates()
    )
    if time_col is not None:
        if time_col not in entity_attribute_data.columns:
            edge_data = edge_data.withColumn(time_col, F.lit(None))
        edge_data = edge_data.select(
            ["Source", "Target", "SourceType", "TargetType", "Weight", time_col]
        )
    else:
        edge_data = edge_data.select(
            ["Source", "Target", "SourceType", "TargetType", "Weight"]
        )

    # fuzzy matches
    if fuzzy_match_data is not None:
        fuzzy_matches = (
            fuzzy_match_data.filter(F.col(attribute_col) == attribute)
            .withColumn("SourceType", F.lit(attribute))
            .withColumn("TargetType", F.lit(attribute))
            .withColumnRenamed("Similarity", "Weight")
            .dropDuplicates()
        )
        if time_col is not None:
            if time_col not in fuzzy_matches.columns:
                fuzzy_matches = fuzzy_matches.withColumn(time_col, F.lit(None))
            fuzzy_matches = fuzzy_matches.select(
                ["Source", "Target", "SourceType", "TargetType", "Weight", time_col]
            )
        else:
            fuzzy_matches = fuzzy_matches.select(
                ["Source", "Target", "SourceType", "TargetType", "Weight"]
            )
        edge_data = edge_data.union(fuzzy_matches)

    # get max weight
    columns = edge_data.columns
    columns.remove("Weight")
    edge_data = edge_data.groupBy(columns).agg(F.max("Weight").alias("Weight"))
    return edge_data


def __compute_exact_match_jaccard_weights(
    attribute_data: DataFrame, 
    attribute: str, 
    entity_col: str = "EntityID"
) -> DataFrame:
    """
    Compute jaccard weights for the entity-entity unipartite graph based on exact matching of entity attributes

    Parameters
    ----------
        attribute_data: Spark DataFrame
            Contains data for a given entity attribute with schema [entity_col, attribute], e.g. [EntityID, Name]
        attribute: str
            Name of the attribute column in the attribute_data dataframe (e.g. 'Name')
        entity_col: str, default = 'EntityID'
            Name of the entity column in the attribute_data dataframe

    Returns
    -------
        exact_matches: Spark DataFrame
            The entity-entity unipartite graph's edge list with schema [Source, Target, Weight]
    """
    # get entity-entity pairs that exactly match on the given attribute
    spark = SparkSession.getActiveSession()
    attribute_data.createOrReplaceTempView(f"{attribute}_temp")
    exact_matches = spark.sql(
        f"""
            SELECT DISTINCT
                tbl1.{entity_col} AS Source,
                tbl2.{entity_col} AS Target
            FROM {attribute}_temp tbl1
            INNER JOIN {attribute}_temp tbl2
                ON trim(tbl1.{attribute}) = trim(tbl2.{attribute})
            WHERE tbl1.{entity_col} != tbl2.{entity_col} 
                AND (trim(tbl1.{attribute}) != '')
                AND (trim(tbl2.{attribute}) != '')
            """
    )

    # get all values of a given attribute for each source/target entity
    entity_attributes = attribute_data.groupby(entity_col).agg(
        F.collect_list(attribute).alias("Attributes")
    )
    exact_matches = exact_matches.join(
        entity_attributes.selectExpr(
            f"{entity_col} AS Source", "Attributes AS SourceAttributes"
        ),
        on="Source",
        how="inner",
    )
    exact_matches = exact_matches.join(
        entity_attributes.selectExpr(
            f"{entity_col} AS Target", "Attributes as TargetAttributes"
        ),
        on="Target",
        how="inner",
    )

    # calculate jaccard similarity as edge weights in the entity-entity graph
    exact_matches = exact_matches.withColumn(
        "Weight",
        similarity_score.jaccard_similarity_udf("SourceAttributes", "TargetAttributes"),
    ).drop("SourceAttributes", "TargetAttributes")
    return exact_matches


def __compute_fuzzy_match_jaccard_weights(
    attribute_data: DataFrame,
    fuzzy_match_data: DataFrame,
    attribute: str,
    entity_col: str = "EntityID",
) -> DataFrame:
    """
    Compute jaccard weights for the entity-entity unipartite graph based on exact matching of entity attributes

    Parameters
    ----------
        attribute_data: Spark DataFrame
            Contains attribute data with schema [entity_col, attribute_col], e.g. [EntityID, Name]
        fuzzy_match_data: Spark DataFrame
            Contain results of fuzzy matching on a single attribute, with schema [Source, Target, Similarity]
        attribute : str
            Name of the entity attribute to peform exact matching, e.g. 'Name'
        entity_col: str, default = 'EntityID'
            Name of the entity column in the attribute_data dataframe

    Returns
    -------
        fuzzy_matches: Spark DataFrame
            The entity-entity unipartite graph's edge list with schema [Source, Target, Weight]
    """
    # get entity-entity pairs that have fuzzy match on the given attribute
    spark = SparkSession.getActiveSession()
    attribute_data.createOrReplaceTempView(f"{attribute}_temp")
    fuzzy_match_data.createOrReplaceTempView(f"{attribute}_fuzzy_temp")
    fuzzy_matches = spark.sql(
        f"""
        SELECT
            tbl1.{entity_col} AS Source,
            tbl2.{entity_col} AS Target,
            SUM(mch.Similarity) AS Intersection
        FROM {attribute}_fuzzy_temp mch
        INNER JOIN {attribute}_temp tbl1
            ON trim(mch.Source) = trim(tbl1.{attribute})
        INNER JOIN {attribute}_temp tbl2
            ON trim(mch.Target) = trim(tbl2.{attribute})
        WHERE (tbl1.{entity_col} != tbl2.{entity_col})
        GROUP BY tbl1.{entity_col}, tbl2.{entity_col}
        """
    )

    # get all values of a given attribute for each source/target entity
    entity_attributes = attribute_data.groupby(entity_col).agg(
        F.collect_list(attribute).alias("Attributes")
    )
    fuzzy_matches = fuzzy_matches.join(
        entity_attributes.selectExpr(
            f"{entity_col} AS Source", "Attributes as SourceAttributes"
        ),
        on="Source",
        how="inner",
    )
    fuzzy_matches = fuzzy_matches.join(
        entity_attributes.selectExpr(
            f"{entity_col} AS Target", "Attributes as TargetAttributes"
        ),
        on="Target",
        how="inner",
    )

    # calculate jaccard similarity as edge weights in the entity-entity unipartite graphs
    fuzzy_matches = fuzzy_matches.withColumn(
        "Union",
        similarity_score.set_union_size_udf("SourceAttributes", "TargetAttributes"),
    ).drop("SourceAttributes", "TargetAttributes")
    fuzzy_matches = fuzzy_matches.withColumn(
        "Weight", F.col("Intersection") / (F.col("Union") - F.col("Intersection"))
    )
    fuzzy_matches = fuzzy_matches.select("Source", "Target", "Weight")
    return fuzzy_matches


def __generate_unipartite_jaccard_edges(
    entity_attribute_data: DataFrame,
    fuzzy_match_data: Union[DataFrame, None],
    attribute: str,
    entity_col: str = "EntityID",
    attribute_col: str = "AttributeID",
    value_col: str = "Value",
) -> DataFrame:
    """
    Convert mutliple attribute data into an edge list of an entity-entity unitpartite graph,
    with jaccard similarity as edge weights

    Parameters
    ----------
        entity_attribute_data: Spark DataFrame
            Contains data of all attributes with schema [entity_col, attribute_col, value_col]
        fuzzy_match_data: Spark DataFrame
            Contain results of fuzzy matching for multiple attributes, with schema [Source, Target, Similarity, attribute_col]
        attribute : str
            Name of the entity attribute to peform exact/fuzzy matching
        entity_col: str, default = 'EntityID'
            Name of the entity column in the entity_attribute_data dataframe
        attribute_col: str, default = 'AttributeID'
            Name of the attribute column in the entity_attribute_data dataframe
        value_col: str, default = 'Value'
            Name of the value column in the entity_attribute_data dataframe

    Returns
    -------
        edge_data: Spark DataFrame
            The entity-entity unipartite graph's edge list with schema [Source, Target, Weight]
    """
    # filter data to the given attribute
    attribute_data = (
        entity_attribute_data.filter(F.col(attribute_col) == attribute)
        .drop(attribute_col)
        .dropDuplicates()
        .withColumnRenamed(value_col, attribute)
    )
    # exact matches
    exact_matches = __compute_exact_match_jaccard_weights(
        attribute_data=attribute_data, attribute=attribute, entity_col=entity_col,
    ).cache()
    logger.info(f"Exact matches: {exact_matches.count()}")

    # fuzzy matches
    if fuzzy_match_data is not None:
        attribute_fuzzy_match_data = (
            fuzzy_match_data.filter(F.col(attribute_col) == attribute)
            .drop(attribute_col)
            .cache()
        )
        fuzzy_matches = __compute_fuzzy_match_jaccard_weights(
            attribute_data=attribute_data,
            fuzzy_match_data=attribute_fuzzy_match_data,
            attribute=attribute,
            entity_col=entity_col,
        ).cache()

        logger.info(f"Fuzzy matches: {fuzzy_matches.count()}")
        edge_data = exact_matches.union(fuzzy_matches)
    else:
        edge_data = exact_matches
    return edge_data


def __generate_unipartite_unnormalized_edges(
    entity_attribute_data: DataFrame,
    fuzzy_match_data: Union[DataFrame, None],
    attribute: str,
    entity_col: str = "EntityID",
    attribute_col: str = "AttributeID",
    value_col: str = "Value",
    exact_match_weight: float = 1.0,
) -> DataFrame:
    """
    Convert entity attribute data into edge list of an entity-entity unitpartite graph,
    using unnormalized exact and fuzzy matching weights as edge weights

    Parameters
    ----------
        entity_attribute_data: Spark DataFrame
            Contains data of all attributes with schema [entity_col, attribute_col, value_col]
        fuzzy_match_data: Spark DataFrame
            Contain results of fuzzy matching on all attribute values, with schema [Source, Target, Similarity, attribute_col]
        attribute : str
            Name of the entity attribute to peform exact matching
        entity_col: str, default = 'EntityID'
            Name of the entity column in the entity_attribute_data dataframe
        attribute_col: str, default = 'AttributeID'
            Name of the attribute column in the entity_attribute_data dataframe
        value_col: str, default = 'Value
            Name of the value column in the entity_attribute_data dataframe
        exact_match_weight: float, default = 1.0
            Default weight for an exact matching edge

    Returns
    -------
        edge_data: Spark DataFrame
            The entity-entity unipartite graph's edge list with schema [Source, Target, Weight]
    """
    # filter data to the given attribute
    attribute_data = (
        entity_attribute_data.filter(F.col(attribute_col) == attribute)
        .drop(attribute_col)
        .dropDuplicates()
        .withColumnRenamed(value_col, attribute)
    )
    attribute_data.createOrReplaceTempView(f"{attribute}_temp")

    # exact matches
    spark = SparkSession.getActiveSession()
    exact_matches = spark.sql(
        f"""
            SELECT DISTINCT
                tbl1.{entity_col} AS Source,
                tbl2.{entity_col} AS Target,
                {exact_match_weight} AS Weight
            FROM {attribute}_temp tbl1
            INNER JOIN {attribute}_temp tbl2
                ON trim(tbl1.{attribute}) = trim(tbl2.{attribute})
            WHERE tbl1.{entity_col} != tbl2.{entity_col} 
                AND (trim(tbl1.{attribute}) != '')
                AND (trim(tbl2.{attribute}) != '')
            """
    )
    logger.info(f"Exact matches: {exact_matches.count()}")

    # fuzzy matches
    if fuzzy_match_data is not None:
        attribute_fuzzy_match_data = (
            fuzzy_match_data.filter(F.col(attribute_col) == attribute)
            .drop(attribute_col)
            .cache()
        )
        attribute_fuzzy_match_data.createOrReplaceTempView(f"{attribute}_fuzzy_temp")
        fuzzy_matches = spark.sql(
            f"""
                SELECT DISTINCT
                    tbl1.{entity_col} AS Source,
                    tbl2.{entity_col} AS Target,
                    mch.Similarity AS Weight
                FROM {attribute}_fuzzy_temp mch
                INNER JOIN {attribute}_temp tbl1
                    ON trim(mch.Source) = trim(tbl1.{attribute})
                INNER JOIN {attribute}_temp tbl2
                    ON trim(mch.Target) = trim(tbl2.{attribute})
                WHERE (tbl1.{entity_col} != tbl2.{entity_col})
                """
        ).cache()

        logger.info(f"Fuzzy matches: {fuzzy_matches.count()}")
        edge_data = exact_matches.union(fuzzy_matches)
    else:
        edge_data = exact_matches

    return edge_data


def convert_links_to_bipartite(  # nosec - B107
    entity_links: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    relationship: str = schemas.RELATIONSHIP,
    entity_col: str = schemas.ENTITY_ID,
    edge_join_token: str = "--",
) -> DataFrame:
    """
    Convert unipartite edge list to a multipartite edge list

    Params:
    -------
        entity_links: Spark DataFrame
            Unipartite edge list with schema [source, target, relationship]
        source_entity: str, default = 'source'
            Name of the source entity column in the entity_links dataframe
        target_entity: str, default = 'target'
            Name of the target entity column in the entity_links dataframe
        relationship: str, default = 'relationship'
            Name of the relationship column in the entity_links dataframe
        entity_col: str, default = 'entity_id'
            Name of the entity column in the entity_links dataframede
        edge_join_token: str, default = '--'
            Token to join source and target entities in the linking node

    Returns:
    --------
        multipartite_links: Spark DataFrame
            Multipartite edge list with schema [source, source_type, target, target_type]
    """
    multipartite_df = entity_links.withColumn(
        "link",
        F.when(
            F.col(source_entity) < F.col(target_entity),
            F.concat(
                F.col(source_entity), F.lit(edge_join_token), F.col(target_entity)
            ),
        ).otherwise(
            F.concat(F.col(target_entity), F.lit(edge_join_token), F.col(source_entity))
        ),
    )
    multipartite_links = multipartite_df.selectExpr(
        source_entity,
        f'"{entity_col}" as {schemas.SOURCE_TYPE}',
        f"link as {schemas.TARGET}",
        f"{relationship} as {schemas.TARGET_TYPE}",
    )

    mirrored_multipartite_links = multipartite_df.selectExpr(
        f"{target_entity} as {schemas.SOURCE}",
        f'"{entity_col}" as {schemas.SOURCE_TYPE}',
        f"link as {schemas.TARGET}",
        f"{relationship} as {schemas.TARGET_TYPE}",
    )
    multipartite_links = multipartite_links.union(mirrored_multipartite_links)
    multipartite_links = multipartite_links.withColumn(
        schemas.WEIGHT, F.lit(1)
    ).dropDuplicates()
    return multipartite_links
