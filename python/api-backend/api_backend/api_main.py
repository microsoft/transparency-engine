#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api_backend.entities.api.router import entities_router
from api_backend.report.api.router import report_router
from api_backend.search.api.router import search_router

app = FastAPI()

enable_cors = os.getenv("ENABLE_CORS", "").strip()

if enable_cors:
    origins = [c.strip() for c in enable_cors.split(",")]
    logging.info(f"Enabling CORS for {origins}")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(search_router, prefix="/api/search")
app.include_router(report_router, prefix="/api/report")
app.include_router(entities_router, prefix="/api/entities")
