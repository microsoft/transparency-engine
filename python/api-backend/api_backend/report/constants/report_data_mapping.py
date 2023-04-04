#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import api_backend.report.builders.activity_analysis as activity_analysis
import api_backend.report.builders.activity_summary as activity_summary
import api_backend.report.builders.entity_overview as entity_overview
import api_backend.report.builders.own_review_flag_analysis as own_review_flag_analysis
import api_backend.report.builders.related_entities as related_entities
import api_backend.report.builders.review_flag_summary as review_flag_summary

report_data_mapping = {
    "attributesummary": {
        "attribute_counts": {"method": entity_overview.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": entity_overview.get_attribute_values, "args": "entity_details"},
    },
    "flagsummary": {"attribute_counts": {"method": review_flag_summary.get_attribute_counts, "args": "raw_section"}},
    "activitysummary": {"attribute_counts": {"method": activity_summary.get_attribute_counts, "args": "raw_section"}},
    "ownflags": {"attribute_values": {"method": own_review_flag_analysis.get_attribute_values, "args": "raw_section"}},
    "directflags": {"attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}},
    "indirectflags": {"attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}},
    "syncactivity": {
        "attribute_counts": {"method": activity_analysis.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "activity",
        },
    },
    "asyncactivity": {
        "attribute_counts": {"method": activity_analysis.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "activity",
        },
    },
}
