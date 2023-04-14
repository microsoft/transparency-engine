#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import api_backend.report.builders.activity_analysis as activity_analysis
import api_backend.report.builders.activity_summary as activity_summary_builder
import api_backend.report.builders.entity_overview as entity_overview
import api_backend.report.builders.own_review_flag_analysis as own_review_flag_analysis
import api_backend.report.builders.related_entities as related_entities
import api_backend.report.builders.review_flag_summary as review_flag_summary
from api_backend.report.constants.attributes import (
    activity_summary,
    async_activity,
    attribute_summary,
    direct_flags,
    flag_summary,
    indirect_flags,
    own_flags,
    sync_activity,
)

report_data_mapping = {
    attribute_summary: {
        "attribute_counts": {"method": entity_overview.get_attribute_counts, "args": "raw_section"},
        "attribute_values": {"method": entity_overview.get_attribute_values, "args": "entity_details"},
    },
    flag_summary: {"attribute_counts": {"method": review_flag_summary.get_attribute_counts, "args": "raw_section"}},
    activity_summary: {
        "attribute_counts": {"method": activity_summary_builder.get_attribute_counts, "args": "raw_section"}
    },
    own_flags: {"attribute_values": {"method": own_review_flag_analysis.get_attribute_values, "args": "raw_section"}},
    direct_flags: {"attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}},
    indirect_flags: {"attribute_values": {"method": related_entities.get_attribute_values, "args": "raw_section"}},
    sync_activity: {
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "activity",
        },
    },
    async_activity: {
        "attribute_values": {"method": activity_analysis.get_attribute_values, "args": "raw_section"},
        "attribute_charts": {
            "method": activity_analysis.get_attribute_charts,
            "args": "activity",
        },
    },
}
