#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import os
import json
from api_backend.report.constants.report_data_mapping import report_data_mapping

constants_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'constants'))


def build_report(id, raw_report, entity_details, activity):
    report = {"html_report": {"reportSections": []}}
    sections = []
    report_template = {} 
    try:
        with open(os.path.join(constants_folder, 'config.json')) as f:
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
    if raw_section is None:
        return section_template
    if section_template is None:
        return None
    if "id" in section_template:
        section_template["id"] = id
    attribute_counts = report_data_mapping.get("attribute_counts", None)
    attribute_values = report_data_mapping.get("attribute_values", None)
    attribute_charts = report_data_mapping.get("attribute_charts", None)
    if attribute_counts is not None:
        data = get_attribute_data(attribute_counts, args)
        section_template["attribute_mapping"]["attribute_counts"]["data"] = data

    if attribute_values is not None:
        data = get_attribute_data(attribute_values, args)
        section_template["attribute_mapping"]["attribute_values"]["data"] = data

    if attribute_charts is not None:
        data = get_attribute_data(attribute_charts, args)
        section_template["attribute_mapping"]["attribute_charts"]["data"] = data

    return section_template


def get_attribute_data(attr, args):
    method = attr.get("method", None)
    arg = attr.get("args", None)
    if attr is None or method is None:
        return []

    return method(args.get(arg, None))
