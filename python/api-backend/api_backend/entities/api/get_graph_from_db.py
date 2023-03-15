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
from api_backend.util.load_env import load_env

load_env()


def get_graph_from_db(source: str, target: Optional[str] = None) -> Graph:
    """
    Retrieves a graph from the database using the given source and target nodes, and returns it as a Graph object.

    Args:
        source: A string representing the source node for the graph.
        target: An optional string representing the target node for the graph.

    Returns:
        A Graph object representing the data retrieved from the database.
    """
    graph_table = os.environ["GRAPH_TABLE"]
    engine = get_engine()
    conn = engine.connect()

    metadata = MetaData()
    table = Table(graph_table, metadata, autoload_with=engine)

    source_where = table.c.source == f"company::{source}"
    expressions = [source_where]

    if target is not None:
        target_where = table.c.target == f"company::{target}"
        expressions.append(target_where)

    query = (
        select(
            table.c.source,
            table.c.target,
            table.c.path_source,
            table.c.path_target,
            table.c.source_color,
            table.c.target_color,
            table.c.source_size,
            table.c.target_size,
        )
        .where(and_(*expressions))
        .distinct()
    )

    graph_df = pd.read_sql(query, conn)

    node_df = (
        graph_df["path_source"]
        .rename("node")
        .append(graph_df["path_target"].rename("node"))
        .drop_duplicates()
        .reset_index(drop=True)
    )

    node_list = node_df.to_list()
    node_to_ids = [(node, index) for index, node in enumerate(node_list)]
    node_id_df = pd.DataFrame(node_to_ids, columns=["node", "id"])

    graph_df2 = pd.merge(
        graph_df,
        (node_id_df.reset_index(drop=True).rename(columns={"node": "path_source", "id": "path_source_id"})),
        on="path_source",
    )

    graph_df2 = pd.merge(
        graph_df2, node_id_df.rename(columns={"node": "path_target", "id": "path_target_id"}), on="path_target"
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
        if row.path_source not in node_dict:
            node_dict[row.path_source] = {
                "id": int(row.path_source_id),
                "name": row.path_source,
                "color": row.source_color,
                "labelColor": "#303030",
                "nodeSize": int(row.source_size),
            }

        if row.path_target not in node_dict:
            node_dict[row.path_target] = {
                "id": int(row.path_target_id),
                "name": row.path_target,
                "color": row.target_color,
                "labelColor": "#303030",
                "nodeSize": int(row.target_size),
            }
        # add edge
        edge_list.append(
            {"source": int(row.path_source_id), "target": int(row.path_target_id), "widthWeight": 1, "colorWeight": 1}
        )

    graph_spec = {"nodes": [node_dict[node] for node in node_dict], "edges": edge_list}
    return graph_spec
