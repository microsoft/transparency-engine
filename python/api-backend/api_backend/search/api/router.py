#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from fastapi import APIRouter

search_router = APIRouter()


@search_router.get("/")
def main():
    return {"message": "search api is healthy"}
