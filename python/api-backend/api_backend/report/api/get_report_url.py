#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from fastapi import status
from fastapi.responses import JSONResponse

from api_backend.report.api.get_report_url_from_db import get_report_url_from_db


async def get_report_url(entity_id):
    """
    Asynchronously retrieves the report url with the specified entity id.

    The function queries a database to retrieve the url of a report and returns it.
    Args:
        id: A string representing the entity id.

    Returns:
        An object with EntityID and ReportLink data
    """

    logging.info(f"Calling get_report_url with param: {id}")

    if not id:
        return JSONResponse(
            content={"error": "No id parameter provided. Please provide valid id."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    try:
        result = get_report_url_from_db(entity_id)
        if result is None:
            return JSONResponse(
                content={"error": f"Not found report url with entity id: {entity_id}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return JSONResponse(content=result, status_code=status.HTTP_200_OK)
    except Exception as e:
        logging.error(e, exc_info=True)
        return JSONResponse(
            content={"error": f"Internal Server Error - {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
