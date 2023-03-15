#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from api_backend.report.constants.report_template import report_data_mapping, report_template


def build_report(raw_report, entity_details, filtered_activity_attributes):
    report = {"html_report": {"reportSections": []}}
    sections = []
    for key in raw_report:
        args = {
            "raw_section": raw_report[key],
            "entity_details": entity_details,
            "filtered_activity_attributes": filtered_activity_attributes.get(key, None),
        }
        if key in report_template:
            sections.append(section_builder(report_template[key], report_data_mapping.get(key, None), args))

    report["html_report"]["reportSections"] = sections
    return report


def section_builder(section_template, report_data_mapping, args):
    raw_section = args.get("raw_section", None)
    if raw_section is None:
        return section_template
    if section_template is None:
        return None
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
