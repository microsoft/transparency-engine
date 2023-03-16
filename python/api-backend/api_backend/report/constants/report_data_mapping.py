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
    "entity_info_details": {
        "attribute_counts": {"method": entity_overview.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": entity_overview.get_attribute_values, "args": "entity_details"},
    },
    "risk_summary_details": {
        "attribute_counts": {"method": review_flag_summary.get_attribute_counts, "args": "raw_section"}
    },
    "activity_summary_details": {
        "attribute_counts": {"method": activity_summary.get_attribute_counts, "args": "raw_section"}
    },
    "own_redflag_details": {
        "attribute_values": {"method": own_review_flag_analysis.get_attribute_values, "args": "raw_section"}
    },
    "direct_redflag_details": {
        "attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}
    },
    "indirect_redflag_details": {
        "attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}
    },
    "sync_entities": {
        "attribute_counts": {"method": activity_analysis.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "filtered_activity_attributes",
        },
    },
    "async_entities": {
        "attribute_counts": {"method": activity_analysis.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "filtered_activity_attributes",
        },
    },
}
