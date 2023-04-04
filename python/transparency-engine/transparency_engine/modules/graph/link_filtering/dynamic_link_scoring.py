#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


import logging

from typing import List, Optional, Tuple, Union

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

import transparency_engine.modules.graph.link_filtering.period_similarity as period_similarity
import transparency_engine.modules.graph.preprocessing.graph_edges as graph_edges
import transparency_engine.pipeline.schemas as schemas

from transparency_engine.modules.graph.link_filtering.normalizer import (
    compute_normalizer,
)
from transparency_engine.modules.graph.link_filtering.period_scoring import (
    aggregate_period_activities,
    compute_all_active_period_summary,
    compute_all_overall_overlap_score,
    compute_all_period_overlap_score,
    compute_overall_overlap_score,
)
from transparency_engine.modules.stats.aggregation import mean_list_udf
from transparency_engine.modules.stats.anomaly_detection import detect_anomaly_zscore


logger = logging.getLogger(__name__)


def mirror_links(input_links: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    Mirrors the links in the input data frame.

    Params:
    -------
        input_links: The input links.
        source_col: The name of the source column.
        target_col: The name of the target column.

    Returns:
    --------
        The mirrored links.
    """
    links = input_links.select(source_col, target_col)
    return (
        links.selectExpr(
            f"{target_col} AS {source_col}", f"{source_col} AS {target_col}"
        )
        .union(links)
        .dropDuplicates()
        .filter(F.col(source_col) < F.col(target_col))
    )


def filter_links(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    sync_attributes: List,
    async_attributes: List,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_attribute_type: str = schemas.TARGET_TYPE,
    activity_time: str = schemas.TIME_PERIOD,
    relationship_type: str = "activity",
    anomaly_threshold: float = 3.0,
    min_overall_similarity: Union[float, None] = None,
    min_normalized_overall_similarity: Union[float, None] = None,
    min_sync_similarity: Union[float, None] = None,
    min_async_similarity: Union[float, None] = None,
) -> DataFrame:
    """
    Filters the links based on the similarity between the attributes of the source and target entities.

    Params:
    -------
        predicted_links: The predicted links.
        activity_data: The activity data.
        sync_attributes: The attributes to use for the synchronous links.
        async_attributes: The attributes to use for the asynchronous links.
        source_entity: The name of the source entity.
        target_entity: The name of the target entity.
        activity_entity: The name of the activity entity.
        activity_attribute: The name of the activity attribute.
        activity_attribute_type: The name of the activity attribute type.
        activity_time: The name of the activity time.
        relationship_type: The type of the relationship.
        anomaly_threshold: The threshold for the anomaly score.

    Returns:
    --------
        The filtered links.

    """
    logger.info("computing sync links")
    sync_links = compute_sync_links(
        predicted_links=predicted_links,
        activity_data=activity_data,
        selected_attributes=sync_attributes,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_attribute_type=activity_attribute_type,
        activity_time=activity_time,
        relationship_type=relationship_type,
        anomaly_threshold=anomaly_threshold,
        min_overall_similarity=min_overall_similarity,
        min_normalized_overall_similarity=min_normalized_overall_similarity,
        min_period_similarity=min_sync_similarity,
    ).cache()
    logger.info(f"sync link count: {sync_links.count()}")

    logger.info("computing async links")
    if (
        sync_attributes
        and async_attributes
        and len(sync_attributes) > 0
        and len(async_attributes) > 0
        and min_overall_similarity is not None
    ):
        async_min_overall_similarity: Optional[float] = (
            min_overall_similarity * len(sync_attributes) / len(async_attributes)
        )
        logger.info(f"async min overall similarity: {async_min_overall_similarity}")
    else:
        async_min_overall_similarity = min_overall_similarity

    async_links = compute_async_links(
        predicted_links=predicted_links,
        activity_data=activity_data,
        selected_attributes=async_attributes,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_attribute_type=activity_attribute_type,
        activity_time=activity_time,
        relationship_type=relationship_type,
        anomaly_threshold=anomaly_threshold,
        min_overall_similarity=async_min_overall_similarity,
        min_normalized_overall_similarity=min_normalized_overall_similarity,
        min_period_similarity=min_async_similarity,
    ).cache()
    logger.info(f"async link count: {async_links.count()}")

    async_links = async_links.join(
        sync_links, on=[source_entity, target_entity], how="leftanti"
    )
    final_links = sync_links.union(async_links)
    return final_links


def compute_sync_links(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    selected_attributes: List,
    source_entity: str,
    target_entity: str,
    activity_entity: str,
    activity_attribute: str,
    activity_attribute_type: str,
    activity_time: str,
    relationship_type: str,
    anomaly_threshold: float,
    min_overall_similarity: Optional[float],
    min_normalized_overall_similarity: Optional[float],
    min_period_similarity: Optional[float],
) -> DataFrame:
    """
    Detect activity links between the source and target entities given a list of synchronous attributes.

    Params:
    -------
        predicted_links: The predicted links.
        activity_data: The activity data.
        selected_attributes: The selected attributes.
        source_entity: The name of the source entity.
        target_entity: The name of the target entity.
        activity_entity: The name of the activity entity.
        activity_attribute: The name of the activity attribute.
        activity_attribute_type: The name of the activity attribute type.
        activity_time: The name of the activity time.
        relationship_type: The type of the relationship.
        anomaly_threshold: The threshold for the anomaly score.
        min_overall_similarity: The minimum overall similarity.
        min_normalized_overall_similarity: The minimum normalized overall similarity.
        min_period_similarity: The minimum period similarity.

    Returns:
    --------
        The detected sync links.
    """
    # compute activity scores for sync attributes
    if selected_attributes:
        selected_activity_data = activity_data.filter(
            F.col(activity_attribute_type).isin(selected_attributes)
        )
    else:
        selected_activity_data = activity_data

    link_scores = compute_activity_scores(
        predicted_links,
        selected_activity_data,
        source_entity,
        target_entity,
        activity_entity,
        activity_attribute,
        activity_time,
        anomaly_threshold,
        min_overall_similarity,
    )
    link_scores = detect_anomaly_zscore(
        df=link_scores,
        col=schemas.NORMALIZED_JACCARD_SIMILARITY,
        outlier_flag_col=schemas.NORMALIZED_JACCARD_ANOMALY,
        min_zscore=anomaly_threshold,
    )
    link_scores = detect_anomaly_zscore(
        df=link_scores,
        col="normalized_sync_similarity",
        outlier_flag_col="normalized_sync_anomaly",
        min_zscore=anomaly_threshold,
    ).cache()
    logger.info(f"finished computing scores for {link_scores.count()} links")

    # select links with abnomally high overall similarity score
    overall_links = link_scores.filter(
        F.col(schemas.NORMALIZED_JACCARD_ANOMALY) == 1
    ).cache()
    logger.info(f"overall similarity links: {overall_links.count()}")

    if min_normalized_overall_similarity is None:
        # set min similarity to average
        min_normalized_overall_similarity = link_scores.agg(
            {schemas.NORMALIZED_JACCARD_SIMILARITY: "mean"}
        ).collect()[0][0]
        logger.info(f"min normalized similarity: {min_normalized_overall_similarity}")

    # select links with abnomally high sync similarity thresholds and overall similarity passing a min threshold
    if (
        min_period_similarity is not None
        and min_normalized_overall_similarity is not None
    ):
        sync_links = link_scores.filter(
            (F.col("normalized_sync_anomaly") == 1)
            & (
                F.col(f"{schemas.SYNC_ACTIVITY_PREFIX}similarity")
                >= min_period_similarity
            )
            & (
                F.col(schemas.NORMALIZED_JACCARD_SIMILARITY)
                >= min_normalized_overall_similarity
            )
        )
        sync_links = sync_links.join(
            overall_links, on=[source_entity, target_entity], how="leftanti"
        ).cache()
        logger.info(f"sync links: {sync_links.count()}")

        final_links = overall_links.union(sync_links)
    else:
        final_links = overall_links
    final_links = final_links.withColumn(
        schemas.RELATIONSHIP,
        F.lit(f"{schemas.SYNC_ACTIVITY_PREFIX}{relationship_type}"),
    )
    final_links = final_links.drop("normalized_sync_anomaly").cache()
    logger.info(f"final sync links: {final_links.count()}")
    return final_links


def compute_async_links(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    selected_attributes: List,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_attribute_type: str = schemas.TARGET_TYPE,
    activity_time: str = schemas.TIME_PERIOD,
    relationship_type: str = "activity",
    anomaly_threshold: float = 3.0,
    min_overall_similarity: Optional[float] = None,
    min_normalized_overall_similarity: Optional[float] = None,
    min_period_similarity: Optional[float] = None,
) -> DataFrame:
    """
    Detect activity links given a list of sync attributes
    """
    # compute activity scores for sync attributes
    if selected_attributes:
        selected_activity_data = activity_data.filter(
            F.col(activity_attribute_type).isin(selected_attributes)
        )
    else:
        selected_activity_data = activity_data

    link_scores = compute_activity_scores(
        predicted_links,
        selected_activity_data,
        source_entity,
        target_entity,
        activity_entity,
        activity_attribute,
        activity_time,
        anomaly_threshold,
        min_overall_similarity,
    ).cache()
    logger.info(f"finished computing scores for {link_scores.count()} links")

    link_scores = detect_anomaly_zscore(
        df=link_scores,
        col=schemas.NORMALIZED_JACCARD_SIMILARITY,
        outlier_flag_col=schemas.NORMALIZED_JACCARD_ANOMALY,
        min_zscore=anomaly_threshold,
    )
    link_scores = detect_anomaly_zscore(
        df=link_scores,
        col="normalized_async_similarity",
        outlier_flag_col="normalized_async_anomaly",
        min_zscore=anomaly_threshold,
    )

    if min_normalized_overall_similarity is None:
        # set min similarity to average
        min_normalized_overall_similarity = link_scores.agg(
            {schemas.NORMALIZED_JACCARD_SIMILARITY: "mean"}
        ).collect()[0][0]
        logger.info(f"min normalized similarity: {min_normalized_overall_similarity}")

    # select links with abnomally high async similarity thresholds and overall similarity passing a min threshold
    async_links = link_scores
    if (
        min_period_similarity is not None
        and min_normalized_overall_similarity is not None
    ):
        async_links = link_scores.filter(
            (F.col("normalized_async_anomaly") == 1)
            & (
                F.col(f"{schemas.ASYNC_ACTIVITY_PREFIX}similarity")
                >= min_period_similarity
            )
            & (
                F.col(schemas.NORMALIZED_JACCARD_SIMILARITY)
                >= min_normalized_overall_similarity
            )
        )

    async_links = async_links.withColumn(
        "relationship", F.lit(f"{schemas.ASYNC_ACTIVITY_PREFIX}{relationship_type}")
    )
    async_links = async_links.drop("normalized_async_anomaly").cache()
    logger.info(f"final async links: {async_links.count()}")
    return async_links


def compute_activity_scores(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    source_entity: str,
    target_entity: str,
    activity_entity: str,
    activity_attribute: str,
    activity_time: str,
    anomaly_threshold: float,
    min_overall_similarity: Optional[float],
) -> DataFrame:
    """
    Compute activity scores for a set of predicted links

    Params:
    -------
        predicted_links: Spark DataFrame
            DataFrame containing predicted links
        activity_data: Spark DataFrame
            DataFrame containing activity data
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
        anomaly_threshold: float
            Threshold for anomaly detection
        min_overall_similarity: float
            Minimum overall similarity threshold for filtering down the dataset

    Returns:
    --------
        Spark DataFrame
            DataFrame containing activity scores for each predicted link
    """
    # compute jaccard and overlap coefficient first so we can filter down the dataset
    # based on jaccard similarity threshold if needed
    overlap_score_df = compute_overall_overlap_score(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
    )
    if min_overall_similarity is not None:
        predicted_links = overlap_score_df.filter(
            (F.col(schemas.JACCARD_SIMILARITY) >= min_overall_similarity)
        ).cache()
    else:
        # filter to only those identified as anomalies
        overlap_score_df = detect_anomaly_zscore(
            df=overlap_score_df,
            col=schemas.JACCARD_SIMILARITY,
            outlier_flag_col=schemas.JACCARD_ANOMALY,
            min_zscore=anomaly_threshold,
        )
        predicted_links = overlap_score_df.filter(
            (F.col(schemas.JACCARD_ANOMALY) == 1)
        ).cache()
    logger.info(f"link count after initial filtering: {predicted_links.count()}")

    logger.info("aggregating data for per-period similarity scores")
    aggregated_df = aggregate_period_activities(
        predicted_links=predicted_links,
        activity_data=activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_time=activity_time,
    ).cache()
    logger.info(f"aggregated record count: {aggregated_df.count()}")

    logger.info("computing normalizer")
    normalizer = compute_normalizer(
        aggregated_activity_data=aggregated_df,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_time=activity_time,
    ).cache()
    logger.info(f"normalizer record count: {normalizer.count()}")

    # sync and async scores
    logger.info("computing sync and async scores")
    period_similarities = period_similarity.compute_period_similarity(
        aggregated_activity_data=aggregated_df,
        source_entity=source_entity,
        target_entity=target_entity,
    ).cache()
    logger.info(f"period similarity record count: {period_similarities.count()}")

    all_scores = (
        overlap_score_df.select(
            [
                source_entity,
                target_entity,
                schemas.JACCARD_SIMILARITY,
                schemas.OVERLAP_SCORE,
                schemas.SHARED,
                f"{schemas.SOURCE}_only",
                f"{schemas.TARGET}_only",
            ]
        )
        .dropDuplicates()
        .join(period_similarities, on=[source_entity, target_entity], how="inner")
    )
    all_scores = all_scores.join(
        normalizer, on=[source_entity, target_entity], how="inner"
    )

    # normalized scores
    logger.info("computing normalized scores")
    all_scores = all_scores.withColumn(
        schemas.NORMALIZED_JACCARD_SIMILARITY,
        F.col("normalizer") * F.col(schemas.JACCARD_SIMILARITY),
    )
    all_scores = all_scores.withColumn(
        "normalized_sync_similarity",
        F.col("normalizer") * F.col(f"{schemas.SYNC_ACTIVITY_PREFIX}similarity"),
    )
    all_scores = all_scores.withColumn(
        "normalized_async_similarity",
        F.col("normalizer") * F.col(f"{schemas.ASYNC_ACTIVITY_PREFIX}similarity"),
    ).cache()
    logger.info(f"normalized score record count: {all_scores.count()}")

    return all_scores


def generate_link_output(  # nosec - B107
    predicted_links: DataFrame,
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    entity_col: str = schemas.ENTITY_ID,
    edge_join_token: str = "--",
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate final link output from the filtered links

    Params:
    -------
        predicted_links: Spark DataFrame
            DataFrame containing the filtered links
        source_entity: str
            Name of the source entity column
        target_entity: str
            Name of the target entity column
        entity_col: str
            Name of the entity column
        attribute_join_token: str
            Token to join the entity and attribute columns
        edge_join_token: str
            Token to join the source and target entity columns

    Returns:
    --------
        Spark DataFrame
            DataFrame containing final links
        Spark DataFrame
            DataFrame containing the multipartite graph output
    """
    final_links_df = predicted_links.select(
        source_entity, target_entity, schemas.RELATIONSHIP
    )
    final_links_df = final_links_df.withColumn(schemas.WEIGHT, F.lit(1))
    final_links_df = final_links_df.withColumn(schemas.SOURCE_TYPE, F.lit(entity_col))
    final_links_df = final_links_df.withColumn(schemas.TARGET_TYPE, F.lit(entity_col))

    # generate multipartite graph output from the final links
    # to be used in the macro link filtering module
    multipartite_edge_data = graph_edges.convert_links_to_bipartite(
        entity_links=final_links_df,
        source_entity=source_entity,
        target_entity=target_entity,
        relationship=schemas.RELATIONSHIP,
        entity_col=entity_col,
        edge_join_token=edge_join_token,
    )
    return (final_links_df, multipartite_edge_data)


def summarize_link_scores(
    predicted_links: DataFrame,
    activity_data: DataFrame,
    link_attributes: List[str],
    source_entity: str = schemas.SOURCE,
    target_entity: str = schemas.TARGET,
    activity_entity: str = schemas.SOURCE,
    activity_attribute: str = schemas.TARGET,
    activity_attribute_type: str = schemas.TARGET_TYPE,
    activity_time: str = schemas.TIME_PERIOD,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate scores needed to summarize a sync or async links, including
    jaccard scores of shared activity and jaccard score on shared periods.

    Params:
        predicted_links: Spark DataFrame
            DataFrame containing the sync/async activity links, with schemas [Source, Target, Paths]
        activity_data: Spark DataFrame
            Dataframe containing the raw activity attributes, with schema [Source, Target, SourceType, TargetType, TimePeriod]
        source_entity: str
            Name of the source entity column in the predicted links dataframe
        target_entity: str
            Name of the target entity column in the predicted links dataframe
        activity_entity: str
            Name of the entity column in the activity dataframe
        activity_attribute: str
            Name of the activity attribute column in the activity dataframe
        activity_time: str
            Name of the activity time period column in the activity_dataframe

    Returns:
    --------
        Spark DataFrame
            DataFrame containing the overall activity summary scores for each activity link
        Spark DataFrame
            DataFrame containing the temporal activity scores for each activity link
    """
    # take half of the links
    input_links = mirror_links(predicted_links, source_entity, target_entity)

    # compute overall scores
    selected_activity_data = activity_data.filter(
        F.col(activity_attribute_type).isin(link_attributes)
    )
    overall_scores = compute_all_overall_overlap_score(
        predicted_links=input_links,
        activity_data=selected_activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
    )

    # compute temporal scores
    temporal_scores = compute_all_period_overlap_score(
        predicted_links=input_links,
        activity_data=selected_activity_data,
        source_entity=source_entity,
        target_entity=target_entity,
        activity_entity=activity_entity,
        activity_attribute=activity_attribute,
        activity_time=activity_time,
    )

    # compute period scores
    period_scores = compute_all_active_period_summary(temporal_scores)
    period_scores = period_scores.select(
        schemas.SOURCE, schemas.TARGET, "type", "period_jaccard_score"
    )

    scores = overall_scores.join(
        period_scores,
        on=[schemas.SOURCE, schemas.TARGET, "type"],
        how="inner",
    ).cache()
    logger.info(f"scores count: {scores.count()}")
    return (scores, temporal_scores)


def get_link_score_summary(scores: DataFrame, attributes: List[str]):
    """
    Flatten the final scores table to be used as input for the reporting function.

    Params:
        scores: DataFrame
            Dataframe with schema [Source, Target, Type, Shared, Source_only, Target_only, JaccardSimilarity, OverlapScore]
        attributes: List[str]
            List of attribute types to summarize

    Returns:
        flatten_scores: DataFrame:
            Flatten dataframe where each column contains the jaccard similarity score for an attribute type.
    """

    period_scores = scores.filter(F.col("type") == "overall").selectExpr(
        schemas.SOURCE, schemas.TARGET, "period_jaccard_score AS period_score"
    )

    attribute_scores = scores.filter(F.col("type").isin(attributes))
    attribute_scores = (
        attribute_scores.groupby([schemas.SOURCE, schemas.TARGET])
        .pivot("type")
        .sum("JaccardSimilarity")
    )
    for attribute in attributes:
        attribute_scores = attribute_scores.withColumnRenamed(
            attribute, f"{attribute}_score"
        )
    attribute_scores = attribute_scores.withColumn(
        "average_score", mean_list_udf(F.array(*[f"{att}_score" for att in attributes]))
    )
    flatten_scores = period_scores.join(
        attribute_scores, on=[schemas.SOURCE, schemas.TARGET], how="inner"
    )
    logger.info("Finished calculating link score summary")
    return flatten_scores
