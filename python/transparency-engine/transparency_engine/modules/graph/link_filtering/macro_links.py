#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import itertools
import logging

from functools import reduce
from typing import Dict, List, Tuple, Union

import networkx as nx
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

import transparency_engine.pipeline.schemas as schemas


logger = logging.getLogger(__name__)


def generate_nx_graph(  # nosec - B107
    multipartite_tables: List[DataFrame],
    source_col: str,
    target_col: str,
    source_type_col: str,
    target_type_col: str,
    weight: float = 1.0,
    join_token: str = "::",
) -> Tuple[nx.Graph, DataFrame]:
    """
    Generate a networkx graph from a list of multipartite tables.

    Params:
    -------
        multipartite_tables: A list of multipartite tables.
        source_col: The name of the source column.
        target_col: The name of the target column.
        source_type_col: The name of the source type column.
        target_type_col: The name of the target type column.
        weight: The weight of the edge. Optional, default is 1.
        join_token: The join token. Optional, default is '::'.

    Returns:
    -------
        graph: A networkx graph.
        edge_data: A Spark DataFrame containing the edge data.
    """

    edge_list: List[DataFrame] = []
    for table in multipartite_tables:
        edge_list.append(
            table.select(
                F.concat(source_type_col, F.lit(join_token), source_col).alias(
                    source_col
                ),
                F.concat(target_type_col, F.lit(join_token), target_col).alias(
                    target_col
                ),
                source_type_col,
                target_type_col,
            ).distinct()
        )

    edge_data = reduce(DataFrame.unionAll, edge_list)
    graph = nx.Graph()
    for edge in edge_data.collect():
        graph.add_node(edge[source_col], partition=edge[source_type_col])
        graph.add_node(edge[target_col], partition=edge[target_type_col])
        graph.add_edge(edge[source_col], edge[target_col], weight=weight)

    return graph, edge_data


def get_valid_links(
    predicted_links: DataFrame,
    graph: nx.Graph,
    edge_data: DataFrame,
    entity_col: str,
    source_entity: str,
    target_entity: str,
    join_token: str,
    noisy_relationships: List,
    max_path_length: int,
    max_fuzzy_chain_length: int,
    max_dynamic_chain_length: int,
    min_direct_links: int,
) -> DataFrame:
    """
    Get valid links from a list of predicted links.

    Params:
    ------
        predicted_links: The predicted links.
        graph: The networkx graph.
        edge_data: The edge data.
        entity_col: The name of the entity column.
        source_entity: The name of the source entity column.
        target_entity: The name of the target entity column.
        join_token: The join token.
        noisy_relationships: A list of noisy relationships.
        max_path_length: The maximum path length.
        max_fuzzy_chain_length: The maximum fuzzy chain length.
        max_dynamic_chain_length: The maximum dynamic chain length.
        min_direct_links: The minimum number of direct links.

    Returns:
    ------
        valid_links: A Spark DataFrame containing the valid links.
    """
    # mirror links and take half
    predicted_links = predicted_links.select(
        F.concat(F.lit(entity_col), F.lit(join_token), source_entity).alias(
            schemas.SOURCE
        ),
        F.concat(F.lit(entity_col), F.lit(join_token), target_entity).alias(
            schemas.TARGET
        ),
    )
    predicted_links = predicted_links.selectExpr(
        f"{source_entity} as {schemas.SOURCE}", f"{target_entity} as {schemas.TARGET}"
    ).dropDuplicates()
    mirrored_links = predicted_links.selectExpr(
        f"{target_entity} as {schemas.SOURCE}", f"{source_entity} as {schemas.TARGET}"
    ).dropDuplicates()
    input_links = predicted_links.union(mirrored_links)
    input_links = input_links.filter(
        F.col(schemas.SOURCE) < F.col(schemas.TARGET)
    ).cache()
    logger.info(f"input link count: {input_links.count()}")

    # get a list of dynamic relationships for link validation
    dynamic_relationship_df = (
        edge_data.select(schemas.TARGET_TYPE)
        .dropDuplicates()
        .filter(
            (F.col(schemas.TARGET_TYPE).contains(schemas.SYNC_ACTIVITY_PREFIX))
            | (F.col(schemas.TARGET_TYPE).contains(schemas.ASYNC_ACTIVITY_PREFIX))
        )
    )
    dynamic_relationships = [
        data[0]
        for data in dynamic_relationship_df.select(schemas.TARGET_TYPE).collect()
    ]
    logger.info(f"dynamic relationships: {dynamic_relationships}")

    simple_path_udf = F.udf(
        lambda source, target: get_simple_paths(
            source,
            target,
            graph,
            entity_col,
            join_token,
            dynamic_relationships,
            noisy_relationships,
            max_path_length,
            max_fuzzy_chain_length,
            max_dynamic_chain_length,
            min_direct_links,
        ),
        ArrayType(ArrayType(StringType())),
    )

    # get paths
    logger.info("computing paths...")
    input_links = input_links.withColumn(
        schemas.PATHS, simple_path_udf(schemas.SOURCE, schemas.TARGET)
    )
    filtered_links = input_links.filter(
        (~F.col(schemas.PATHS).isNull()) & (F.size(F.col(schemas.PATHS)) > 0)
    )

    # mirror links
    final_links = filtered_links.select(schemas.SOURCE, schemas.TARGET, schemas.PATHS)
    mirrored_links = final_links.selectExpr(
        f"{schemas.TARGET} as {schemas.SOURCE}",
        f"{schemas.SOURCE} as {schemas.TARGET}",
        schemas.PATHS,
    )
    final_links = final_links.union(mirrored_links).dropDuplicates()
    return final_links


def get_simple_paths(
    source: str,
    target: str,
    graph: nx.Graph,
    entity_col: str,
    join_token: str,
    dynamic_relationships: List[str],
    noisy_relationships: List[str],
    max_path_length: int,
    max_fuzzy_chain_length: int,
    max_dynamic_chain_length: int,
    min_direct_links: int,
) -> Union[List[List[str]], None]:
    """
    Get simple paths between two nodes.

    Params:
    ----------
        source: The source node.
        target: The target node.
        graph: The networkx graph.
        entity_col: The name of the entity column.
        join_token: The join token.
        dynamic_relationships: A list of dynamic relationships.
        noisy_relationships: A list of noisy relationships.
        max_path_length: The maximum path length.
        max_fuzzy_chain_length: The maximum fuzzy chain length.
        max_dynamic_chain_length: The maximum dynamic chain length.
        min_direct_links: The minimum number of direct links.

    Returns:
    ----------
        simple_paths: A list of simple paths.

    """
    if (
        source in graph.nodes()
        and target in graph.nodes()
        and nx.has_path(graph, source, target)
    ):
        simple_paths = list(
            nx.all_simple_paths(graph, source, target, cutoff=max_path_length)
        )
        simple_paths = sorted(simple_paths, key=len)
        cleaned_simple_paths: List[List[str]] = []
        for path in simple_paths:
            is_valid_path = True
            for cleaned_path in cleaned_simple_paths:
                if set(cleaned_path).issubset(set(path)):
                    is_valid_path = False
                    break
            if is_valid_path:
                fuzzy_check = has_long_fuzzy_chains(
                    path, entity_col, join_token, max_fuzzy_chain_length
                )

                dynamic_check = has_long_dynamic_chains(
                    path, dynamic_relationships, join_token, max_dynamic_chain_length
                )
                if fuzzy_check == 0 and dynamic_check == 0:
                    cleaned_simple_paths.append(path)

        if noisy_relationships:
            cleaned_simple_paths = remove_low_confidence_links(
                cleaned_simple_paths,
                noisy_relationships,
                join_token,
                entity_col,
                max_path_length,
            )

        # remove those without min number of direct links
        if has_direct_links(cleaned_simple_paths, min_direct_links):
            return cleaned_simple_paths
        else:
            return None
    else:
        return None


def has_fuzzy_link(path: List, entity_col: str, join_token: str) -> int:
    """
    Check if a path has a fuzzy link.

    Params:
    -------
        path: A list of nodes.
        entity_col: The name of the entity column.
        join_token: The join token.

    Returns:
    -------
        1 if the path has a fuzzy link, 0 otherwise.
    """
    for index in range(len(path) - 1):
        if path[index].split(join_token)[0] == path[index + 1].split(join_token)[0]:
            return 1
    return 0


def has_long_fuzzy_chains(
    path: List, entity_col: str, join_token: str, max_fuzzy_chain_length: int
) -> int:
    """
    Check if a path has a long fuzzy chain.

    Params:
    -------
        path: A list of nodes.
        entity_col: The name of the entity column.

    Returns:
    -------
        1 if the path has a long fuzzy chain, 0 otherwise.
    """
    for index in range(len(path) - max_fuzzy_chain_length - 1):
        node_chain = path[index : index + max_fuzzy_chain_length + 2]
        long_fuzzy_chain = True
        for node in node_chain:
            if node.split(join_token)[0] == entity_col:
                long_fuzzy_chain = False
                break
        if long_fuzzy_chain:
            return 1
    return 0


def has_long_dynamic_chains(
    path: List,
    dynamic_relationships: List,
    join_token: str,
    max_dynamic_chain_length: int,
) -> int:
    """
    Check if a path has a long dynamic chain.

    Params:
    -----
        path: A list of nodes.
        dynamic_relationships: A list of dynamic relationships.
        join_token: The join token.
        max_dynamic_chain_length: The maximum dynamic chain length.

    Returns:
    -------
        1 if the path has a long dynamic chain, 0 otherwise.
    """
    dynamic_chain_length = 0
    for node in path:
        if node.split(join_token)[0] in dynamic_relationships:
            dynamic_chain_length += 1
    if dynamic_chain_length > max_dynamic_chain_length:
        return 1
    else:
        return 0


def has_direct_links(paths: List[List], min_direct_links: int):
    """
    Check if a path has direct links.

    Params:
    -----
        paths: A list of paths.
        min_direct_links: The minimum number of direct links.

    Returns:
    -----
        1 if the path has direct links, 0 otherwise.
    """
    if not paths:
        return 0
    direct_link_count = 0
    for path in paths:
        # take into account
        if len(path) <= 4:
            direct_link_count += 1
    if direct_link_count >= min_direct_links:
        return 1
    else:
        return 0


def remove_noisy_dynamic_links(
    paths: List[List], join_token: str
) -> Tuple[Union[List[List], None], bool]:
    """
    Remove noisy dynamic links.
    If one of the paths include dynamic links, it should be supported by at least 1 non-dynamic relationship

    Params:
    -----
        paths: A list of paths.
        join_token: The join token.

    Returns:
    -----
        A tuple of (cleaned_paths, is_dynamic_path)
    """
    if not paths:
        return (None, False)

    dynamic_path_count = 0
    non_dynamic_path_count = 0
    for path in paths:
        is_dynamic_path = False
        for node in path:
            node_type = node.split(join_token)[0]
            if (
                schemas.SYNC_ACTIVITY_PREFIX in node_type
                or schemas.ASYNC_ACTIVITY_PREFIX in node_type
            ):
                is_dynamic_path = True
                break
        if is_dynamic_path:
            dynamic_path_count += 1
        else:
            non_dynamic_path_count += 1
    if dynamic_path_count >= 1 and non_dynamic_path_count == 0:
        return (None, True)
    elif dynamic_path_count == 0:
        return (paths, False)
    else:
        return (paths, True)


def extract_entity_links(
    paths: List[List], join_token: str, entity_col: str, max_path_length: int
) -> Tuple[Dict, List]:
    """
    Extract entity links from paths.

    Params:
    -----
        paths: A list of paths.
        join_token: The join token.
        entity_col: The name of the entity column.
        max_path_length: The maximum path length.

    Returns:
    -----
        A tuple of (entity_links, all_entity_pairs)
    """
    entity_links = {}
    all_entity_pairs = []  # list of list of entity pairs
    entity_pair_set = set()
    entity_graph = nx.Graph()
    for path in paths:
        # find tuples of entityA-attribute-entityB, or entityA-attribute-attribute-entityB
        entities = set()
        for node_index in range(len(path) - 1):
            entity_graph.add_edge(path[node_index], path[node_index + 1], weight=1)
            if path[node_index].split(join_token)[0] == entity_col:
                entities.add(path[node_index])

        # add last entity
        entities.add(path[len(path) - 1])

        # generate all pairs of entities for this path
        sorted_entities = sorted(list(entities))
        entity_pairs = list(itertools.combinations(sorted_entities, 2))
        all_entity_pairs.append(entity_pairs)

        for pair in entity_pairs:
            entity_pair_set.add(pair)

    # for each pair of entities, find all simple paths
    for pair in list(entity_pair_set):
        simple_paths = list(
            nx.all_simple_paths(entity_graph, pair[0], pair[1], cutoff=max_path_length)
        )
        simple_paths = sorted(simple_paths, key=len)
        entity_links[pair] = simple_paths

    return (entity_links, all_entity_pairs)


def remove_low_confidence_links(
    paths: List[List],
    noisy_relationships: List[str],
    join_token: str,
    entity_col: str,
    max_path_length: int,
) -> List[List]:
    """
    Remove low confidence links.
    - High confidence links = entity-entity links with at least 1 non-noisy path

    Params:
    -----
        paths: A list of paths.
        noisy_relationships: A list of noisy relationships.
        join_token: The join token.
        entity_col: The name of the entity column.
        max_path_length: The maximum path length.

    Returns:
    -----
        A list of cleaned paths.
    """

    # First, collect all connections between an entity-entity link
    entity_links, all_entity_pairs = extract_entity_links(
        paths, join_token, entity_col, max_path_length
    )

    # Remove low confidence paths
    return [
        path
        for path, entity_pairs in zip(paths, all_entity_pairs)
        if all(
            any(
                not has_fuzzy_link(link, entity_col, join_token)
                and all(
                    node.split(join_token)[0] not in noisy_relationships
                    for node in link
                )
                for link in entity_links[pair]
            )
            for pair in entity_pairs
        )
    ]


def add_missing_links(
    predicted_links: DataFrame,
    graph: nx.Graph,
    edge_data: DataFrame,
    source_entity: str,
    target_entity: str,
    related_entities: str,
    entity_col: str,
    join_token: str,
    noisy_relationships: List[str],
    max_path_length: int,
    max_fuzzy_chain_length: int,
    max_dynamic_chain_length: int,
    min_direct_links: int,
) -> DataFrame:
    """
    Add missing links to the predicted links.

    Params:
    ------
        predicted_links: A dataframe of predicted links.
        graph: The graph.
        edge_data: The edge data.
        source_entity: The name of the source entity column.
        target_entity: The name of the target entity column.
        related_entities: The name of the related entities column.
        entity_col: The name of the entity column.
        join_token: The join token.
        max_path_length: The maximum path length.
        max_fuzzy_chain_length: The maximum fuzzy chain length.
        max_dynamic_chain_length: The maximum dynamic chain length.
        min_direct_links: The minimum number of direct links.

    Returns:
    -------
        A dataframe of predicted links with missing links added.
    """
    missing_links = get_missing_links(predicted_links)
    if missing_links.count() > 0:
        logger.info(f"missing_link count: {missing_links.count()}")
        valid_links = get_valid_links(
            predicted_links=missing_links,
            graph=graph,
            edge_data=edge_data,
            source_entity=source_entity,
            target_entity=target_entity,
            entity_col=entity_col,
            join_token=join_token,
            noisy_relationships=noisy_relationships,
            max_path_length=max_path_length,
            max_fuzzy_chain_length=max_fuzzy_chain_length,
            max_dynamic_chain_length=max_dynamic_chain_length,
            min_direct_links=min_direct_links,
        ).cache()

        logger.info(f"new link count: {valid_links.count()}")

        # filter out paths that contain new entities
        valid_links = valid_links.join(
            missing_links, on=[source_entity, target_entity], how="inner"
        )
        valid_links = valid_links.withColumn(
            "paths",
            remove_unrelated_paths_udf(
                F.col(schemas.PATHS),
                F.col(related_entities),
                F.lit(join_token),
                F.lit(entity_col),
            ),
        ).drop(related_entities)
        valid_links = valid_links.filter(
            (~F.col(schemas.PATHS).isNull()) & (F.size(F.col(schemas.PATHS)) > 0)
        ).cache()
        logger.info(f"new link count post-filtering: {valid_links.count()}")

        predicted_links = predicted_links.union(valid_links)
    predicted_links = predicted_links.withColumn(
        schemas.SOURCE, F.split(F.col(schemas.SOURCE), join_token).getItem(1)
    )
    predicted_links = predicted_links.withColumn(
        schemas.TARGET, F.split(F.col(schemas.TARGET), join_token).getItem(1)
    )

    return predicted_links


def get_missing_links(predicted_links: DataFrame) -> DataFrame:
    """
    Get missing links from predicted links.
    - Missing links = links that are not in the predicted links

    Params:
    --------
        predicted_links: A DataFrame of predicted links.

    Returns:
    --------
        A DataFrame of missing links.
    """
    source_network_df = predicted_links.groupBy(schemas.SOURCE).agg(
        F.collect_list(schemas.PATHS).alias(schemas.PATHS)
    )
    source_network_df = source_network_df.withColumn(
        schemas.RELATED, get_related_entities_udf(schemas.PATHS)
    )
    source_network_df = source_network_df.select(
        schemas.SOURCE,
        schemas.RELATED,
        F.explode(schemas.RELATED).alias(schemas.TARGET),
    ).dropDuplicates()
    source_network_df = source_network_df.filter(
        F.col(schemas.SOURCE) != F.col(schemas.TARGET)
    )
    missing_links = source_network_df.join(
        predicted_links, on=[schemas.SOURCE, schemas.TARGET], how="leftanti"
    ).dropDuplicates()
    return missing_links


@udf(returnType=ArrayType(ArrayType(StringType())))
def remove_unrelated_paths_udf(
    paths: List, related_entities: List, join_token: str, entity_col: str
) -> Union[List[List[str]], None]:
    """
    Remove paths that contain unrelated entities.

    Params:
    -----
        paths: A list of paths.
        related_entities: A list of related entities.
        join_token: The join token.
        entity_col: The name of the entity column.

    Returns:
    -----
        A list of cleaned paths.
    """
    if not paths:
        return None
    else:
        cleaned_paths = []
        for path in paths:
            is_valid_path = True
            if related_entities:
                for node in path:
                    if (
                        node.split(join_token)[0] == entity_col
                        and node not in related_entities
                    ):
                        is_valid_path = False
                        break
                if is_valid_path:
                    cleaned_paths.append(path)
            else:
                cleaned_paths.append(path)
        return cleaned_paths


@udf(returnType=ArrayType(StringType()))
def get_related_entities_udf(paths: List[List[List[str]]]) -> List[str]:
    """
    Get related entities from paths.

    Params:
    -----
        paths: A list of paths.

    Returns:
    -------
        A list of related entities.
    """
    related_list: List[str] = []
    flatten_paths_list: List[List[str]] = list(itertools.chain(*paths))
    flatten_paths: List[str] = list(itertools.chain(*flatten_paths_list))
    related_list = list(
        set([entity for entity in flatten_paths if entity.split("::")[0] == "company"])
    )
    return related_list
