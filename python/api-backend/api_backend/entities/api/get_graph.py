#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging
from typing import Optional

from fastapi import status
from fastapi.responses import JSONResponse

from api_backend.entities.api.get_graph_from_db import get_graph_from_db, get_graph_schema_from_db

error_text = "No source parameter provided. Please provide valid source."


async def get_graph(source: str, target: Optional[str] = None):
    """
    Asynchronously retrieves a graph from the database using the given source and target nodes, and returns it.

    Args:
        source: A string representing the source node for the graph.
        target: An optional string representing the target node for the graph.

    Returns:
        The graph retrieved from the database.
    """
    logging.info(f"Calling get_graph with params: {source}, {target}")

    if not source:
        return JSONResponse(content={"error": error_text}, status_code=status.HTTP_400_BAD_REQUEST)

    try:
        results = get_graph_from_db(source, target)
        return JSONResponse(content=results, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.error(e, exc_info=True)
        return JSONResponse(
            content={"error": f"Internal Server Error - {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


async def get_graph_schema(source: str, target: Optional[str] = None):
    """
    Asynchronously retrieves a graph from the database using the given source and target nodes, and returns it.

    Args:
        source: A string representing the source node for the graph.
        target: An optional string representing the target node for the graph.

    Returns:
        The graph retrieved from the database.
    """
    logging.info(f"Calling get_graph_schema with params: {source}, {target}")

    if not source:
        return JSONResponse(content={"error": error_text}, status_code=status.HTTP_400_BAD_REQUEST)

    try:
        results = get_graph_schema_from_db(source, target)

        if not results:
            return JSONResponse(content={"error": "No graph schema found."}, status_code=status.HTTP_404_NOT_FOUND)
        return JSONResponse(content=results, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.error(e, exc_info=True)
        return JSONResponse(
            content={"error": f"Internal Server Error - {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
