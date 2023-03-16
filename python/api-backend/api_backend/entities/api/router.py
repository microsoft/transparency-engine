#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from fastapi import APIRouter, Query

from api_backend.entities.api.get_graph import get_graph

entities_router = APIRouter()


@entities_router.get("/health")
async def main():
    return {"message": "entities api is healthy"}


@entities_router.get("/graph/{source}")
async def graph(source, target=Query(None)):
    return await get_graph(source, target)
