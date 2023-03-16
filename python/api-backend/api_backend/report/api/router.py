#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from fastapi import APIRouter

from api_backend.report.api.get_report import get_report

report_router = APIRouter()


@report_router.get("/health")
async def main():
    return {"message": "report api is healthy"}


@report_router.get("/{id}")
async def report(id):
    return await get_report(id)
