#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import os

from sqlalchemy import MetaData, Table, select

from api_backend.report.builders.report import build_report
from api_backend.report.util.parsers import parse_entity_details, parse_activity, parse_raw_report
from api_backend.util.db_engine import get_engine


def get_report_from_db(id=""):
    report_table_name = os.getenv("REPORT_TABLE", "")
    entity_table_name = os.getenv("ENTITY_TABLE", "")
    filtered_attributes_table_name = os.getenv("ACTIVITY_TABLE", "")
    engine = get_engine()

    metadata = MetaData()
    report_table = Table(report_table_name, metadata, autoload_with=engine)
    entity_table = Table(entity_table_name, metadata, autoload_with=engine)
    filtered_attributes_table = Table(filtered_attributes_table_name, metadata, autoload_with=engine)

    query_report = select(report_table).where(report_table.c.EntityID == id)

    query_entity = select(entity_table).where(entity_table.c.EntityID == id)

    query_filtered_attributes = lambda id_list: select(filtered_attributes_table).where(
        filtered_attributes_table.c.EntityID.in_(id_list)
    )

    conn = engine.connect()

    result_proxy = conn.execute(query_report)
    report_results = result_proxy.fetchall()
    column_names = result_proxy.keys()
    entity_results = conn.execute(query_entity).fetchall()
    entity_details = parse_entity_details(entity_results)
    raw_report = parse_raw_report(column_names, report_results)
    activity = {}

    if raw_report.get("syncactivity", None):
        entity_ids = [id] + [item["related"] for item in raw_report["syncactivity"]]
        activity_results = conn.execute(query_filtered_attributes(entity_ids)).fetchall()
        activity = {
            "syncactivity": parse_activity(activity_results, id)
        }
    if raw_report.get("asyncactivity", None):
        entity_ids = [id] + [item["related"] for item in raw_report["asyncactivity"]]
        activity_results = conn.execute(query_filtered_attributes(entity_ids)).fetchall()
        activity = {
            "asyncactivity": parse_activity(activity_results, id)
        }

    conn.close()
    return build_report(id, raw_report, entity_details, activity)
