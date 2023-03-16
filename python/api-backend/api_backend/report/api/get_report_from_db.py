#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import os

from sqlalchemy import MetaData, Table, select

from api_backend.report.builders.report import build_report
from api_backend.report.util.parsers import parse_entity_details, parse_filtered_activity_attributes, parse_raw_report
from api_backend.util.db_engine import get_engine


def get_report_from_db(id=""):
    report_table_name = os.getenv("RAW_REPORT_TABLE", "")
    entity_table_name = os.getenv("ENTITY_TABLE", "")
    filtered_attributes_table_name = os.getenv("ACTIVITY_TABLE", "")
    engine = get_engine()

    metadata = MetaData()
    report_table = Table(report_table_name, metadata, autoload_with=engine)
    entity_table = Table(entity_table_name, metadata, autoload_with=engine)
    filtered_attributes_table = Table(filtered_attributes_table_name, metadata, autoload_with=engine)

    query_report = select(report_table).where(report_table.c.company == f"company::{id}")

    query_entity = select(entity_table).where(entity_table.c.company == f"company::{id}")

    query_filtered_attributes = lambda id_list: select(filtered_attributes_table).where(
        filtered_attributes_table.c.company.in_(id_list)
    )

    conn = engine.connect()

    result_proxy = conn.execute(query_report)
    report_results = result_proxy.fetchall()
    column_names = result_proxy.keys()
    entity_results = conn.execute(query_entity).fetchall()
    entity_details = parse_entity_details(entity_results)
    raw_report = parse_raw_report(column_names, report_results)
    filtered_activity_attributes = {}

    if raw_report.get("sync_entities", None):
        entity_ids = [f"company::{id}"] + [item["related"] for item in raw_report["sync_entities"]]
        filtered_activity_attributes_results = conn.execute(query_filtered_attributes(entity_ids)).fetchall()
        filtered_activity_attributes = {
            "sync_entities": parse_filtered_activity_attributes(filtered_activity_attributes_results, id)
        }
    if raw_report.get("async_entities", None):
        entity_ids = [f"company::{id}"] + [item["related"] for item in raw_report["async_entities"]]
        filtered_activity_attributes_results = conn.execute(query_filtered_attributes(entity_ids)).fetchall()
        filtered_activity_attributes = {
            "async_entities": parse_filtered_activity_attributes(filtered_activity_attributes_results, id)
        }

    conn.close()
    return build_report(id, raw_report, entity_details, filtered_activity_attributes)
