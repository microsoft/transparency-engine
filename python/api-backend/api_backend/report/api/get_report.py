#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from fastapi import status
from fastapi.responses import JSONResponse

from api_backend.report.api.get_report_from_db import get_report_from_db


async def get_report(id: str):
    """
    Asynchronously retrieves a report with the specified entity id.

    The function queries a database to retrieve the data to build a report with the specified id and returns it.

    Args:
        id: A string representing the entity id.

    Returns:
        A report object.
    """

    logging.info(f"Calling get_report with param: {id}")

    if not id:
        return JSONResponse(
            content={"error": "No id parameter provided. Please provide valid id."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        results = get_report_from_db(id)
        return JSONResponse(content=results, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.error(e, exc_info=True)
        return JSONResponse(
            content={"error": f"Internal Server Error - {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
