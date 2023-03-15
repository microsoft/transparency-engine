#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from fastapi import APIRouter, Query

from api_backend.report.api.get_report import get_report

report_router = APIRouter()


@report_router.get("/health")
async def main():
    return {"message": "report api is healthy"}


@report_router.get("/")
async def report(id=Query(None)):
    return await get_report(id)
