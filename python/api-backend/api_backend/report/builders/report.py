#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import json
import os

from api_backend.report.constants.report_data_mapping import report_data_mapping

constants_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "constants"))
no_results_message = "No results reported."


def build_report(id, raw_report, entity_details, activity):
    report = {"html_report": {"reportSections": []}}
    sections = []
    report_template = {}
    try:
        with open(os.path.join(constants_folder, "config.json")) as f:
            report_template = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON syntax in config file") from e
    for key in raw_report:
        args = {
            "raw_section": raw_report[key],
            "entity_details": entity_details,
            "activity": activity.get(key, None),
        }
        if key in report_template:
            sections.append(section_builder(id, report_template[key], report_data_mapping.get(key, None), args))

    report["html_report"]["reportSections"] = sections
    return report


def section_builder(id, section_template, report_data_mapping, args):
    raw_section = args.get("raw_section", None)
    attribute_counts = report_data_mapping.get("attribute_counts", None)
    attribute_values = report_data_mapping.get("attribute_values", None)
    attribute_charts = report_data_mapping.get("attribute_charts", None)
    data_mapping = {
        "attribute_counts": False,
        "attribute_values": False,
        "attribute_charts": False,
    }

    if raw_section is None:
        section_template["intro"] = no_results_message
        if attribute_counts is not None:
            del section_template["attribute_mapping"]["attribute_counts"]
        if attribute_values is not None:
            del section_template["attribute_mapping"]["attribute_values"]
        if attribute_charts is not None:
            del section_template["attribute_mapping"]["attribute_charts"]
        return section_template

    if section_template is None:
        return None

    if "id" in section_template:
        section_template["id"] = id

    if attribute_counts is not None:
        data = get_attribute_data(attribute_counts, args)
        section_template["attribute_mapping"]["attribute_counts"]["data"] = data
        if not data:
            section_template["attribute_mapping"]["attribute_counts"] = update_no_data_attributes(
                section_template, "attribute_counts"
            )
        else:
            data_mapping["attribute_counts"] = True
    if attribute_values is not None:
        data = get_attribute_data(attribute_values, args)
        section_template["attribute_mapping"]["attribute_values"]["data"] = data
        if not data:
            section_template["attribute_mapping"]["attribute_values"] = update_no_data_attributes(
                section_template, "attribute_values"
            )
        else:
            data_mapping["attribute_values"] = True

    if attribute_charts is not None:
        data = get_attribute_data(attribute_charts, args)
        section_template["attribute_mapping"]["attribute_charts"]["data"] = data
        if not data:
            section_template["attribute_mapping"]["attribute_charts"] = update_no_data_attributes(
                section_template, "attribute_charts"
            )
        else:
            data_mapping["attribute_charts"] = True

    if not any(data_mapping.values()):
        section_template["intro"] = ""

    return section_template


def get_attribute_data(attr, args):
    method = attr.get("method", None)
    arg = attr.get("args", None)
    if attr is None or method is None:
        return []

    return method(args.get(arg, None))


def update_no_data_attributes(section_template, attribute):
    section_template["attribute_mapping"][attribute]["intro"] = no_results_message
    if "columns" in section_template["attribute_mapping"][attribute]:
        del section_template["attribute_mapping"][attribute]["columns"]
    return section_template["attribute_mapping"][attribute]
