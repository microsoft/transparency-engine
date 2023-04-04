#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from fastapi import status
from fastapi.responses import JSONResponse

from api_backend.report.api.get_report_score_from_db import get_report_score_from_db


async def get_report_score(id: str):
    """
    Asynchronously retrieves the report score for the specified entity id.

    The function queries a database to retrieve the network score data.

    Args:
        id: A string representing the entity id.

    Returns:
        A network score object.
    """

    logging.info(f"Calling get_report score with param: {id}")

    if not id:
        return JSONResponse(
            content={"error": "No id parameter provided. Please provide valid id."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        result = get_report_score_from_db(id)
        if result is None:
            return JSONResponse(
                content={"error": f"Not found report score with entity id: {id}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return JSONResponse(content=result, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.error(e, exc_info=True)
        return JSONResponse(
            content={"error": f"Internal Server Error - {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
