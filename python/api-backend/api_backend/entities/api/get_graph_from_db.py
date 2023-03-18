#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import os
from typing import Optional

import pandas as pd
from sqlalchemy import MetaData, Table, and_, select

from api_backend.model.graph_model import Graph
from api_backend.util.db_engine import get_engine


def get_graph_from_db(source: str, target: Optional[str] = None) -> Graph:
    """
    Retrieves a graph from the database using the given source and target nodes, and returns it as a Graph object.

    Args:
        source: A string representing the source node for the graph.
        target: An optional string representing the target node for the graph.

    Returns:
        A Graph object representing the data retrieved from the database.
    """
    graph_table = os.getenv("GRAPH_TABLE", "")
    engine = get_engine()
    conn = engine.connect()

    metadata = MetaData()
    table = Table(graph_table, metadata, autoload_with=engine)

    source_where = table.c.Source == source
    expressions = [source_where]

    if target is not None:
        target_where = table.c.Target == target
        expressions.append(target_where)

    query = (
        select(
            table.c.Source,
            table.c.Target,
            table.c.PathSource,
            table.c.PathTarget,
            table.c.PathSourceType,
            table.c.PathTargetType,
            table.c.PathSourceFlag,
            table.c.PathTargetFlag,
            table.c.PathSourceRelationship,
            table.c.PathTargetRelationship,
        )
        .where(and_(*expressions))
        .distinct()
    )

    graph_df = pd.read_sql(query, conn)

    node_df = (
        graph_df["PathSource"]
        .rename("node")
        .append(graph_df["PathTarget"].rename("node"))
        .drop_duplicates()
        .reset_index(drop=True)
    )

    node_list = node_df.to_list()
    node_to_ids = [(node, index) for index, node in enumerate(node_list)]
    node_id_df = pd.DataFrame(node_to_ids, columns=["node", "id"])

    graph_df2 = pd.merge(
        graph_df,
        (node_id_df.reset_index(drop=True).rename(columns={"node": "PathSource", "id": "PathSourceId"})),
        on="PathSource",
    )

    graph_df2 = pd.merge(
        graph_df2, node_id_df.rename(columns={"node": "PathTarget", "id": "PathTargetId"}), on="PathTarget"
    )

    conn.close()

    return generate_graph_specs(graph_df2)


def generate_graph_specs(graph_df: pd.DataFrame):
    """
    Generate graph specifications.

    Args:
        graph_df (pd.DataFrame): The DataFrame to generate the graph specifications for.

    Returns:
        dict: A dictionary containing the graph nodes and edges.
    """
    node_dict = {}
    edge_list = []

    for row in graph_df.itertuples():
        # add path source and path target
        if row.PathSource not in node_dict:
            node_dict[row.PathSource] = {
                "id": int(row.PathSourceId),
                "name": row.PathSource,
                "type": row.PathSourceType,
                "relationship": row.PathSourceRelationship,
                "flag": row.PathSourceFlag
            }

        if row.PathTarget not in node_dict:
            node_dict[row.PathTarget] = {
                "id": int(row.PathTargetId),
                "name": row.PathTarget,
                "type": row.PathTargetType,
                "relationship": row.PathTargetRelationship,
                "flag": row.PathTargetFlag

            }
        # add edge
        edge_list.append(
            {"source": int(row.PathSourceId), "target": int(row.PathTargetId)}
        )

    graph_spec = {"nodes": [node_dict[node] for node in node_dict], "edges": edge_list}
    return graph_spec
