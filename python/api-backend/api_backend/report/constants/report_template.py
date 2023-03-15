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
from api_backend.model.report_model import AttributeType, FormatType

attribute_counts_columns = ["Attribute", "Count"]
attribute_values_columns = ["Attribute", "Value"]
flag_columns = ["Flag", "Evidence"]
percent_rank_columns = ["Attribute", "Count", "Percent rank"]
flag_details_columns = ["Entity ID", "Relationship Paths", "Review Flags"]

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

report_template = {
    "entity_report_intro": {
        "title": "ENTITY REPORT",
        "intro": 'This Transparency Engine report illustrates how risks or opportunities may transfer to an entity of interest via its network of closely related entities. Close relationships between pairs of entities are determined through statistical analysis of shared attributes and activity, while review flags ("red flag" risks or "green flag" opportunities) are provided as data inputs.',

    },
    "entity_info_details": {
        "title": "Entity overview",
        "intro": 'Summary of attributes associated with the target entity in the input data.',

        "attribute_mapping": {
            "attribute_counts": {
                "title": "Attribute Counts",
                "intro": "", 
                "type": AttributeType.COUNT.value,
                "format": FormatType.TABLE.value,
                "columns": attribute_counts_columns,
                "data": [],
            },
            "attribute_values": {
                "title": "Attribute Values",
                "intro": "",  
                "type": AttributeType.LIST.value,
                "format": FormatType.TABLE.value,
                "columns": attribute_values_columns,
                "data": [],
            },
        },
    },
    "risk_summary_details": {
        "title": "Review flag summary",
        "intro": "Summary of review flags associated with the target entity and its broader network of closely-related entities.",  

        "attribute_mapping": {
            "attribute_counts": {
                "title": "", 
                "intro": "",  
                "type": AttributeType.COUNT_PERCENT_RANK.value,
                "format": FormatType.TABLE.value,
                "columns": percent_rank_columns,
                "data": [],
            }
        },
    },
    "activity_summary_details": {
        "title": "Activity summary",
        "intro": "Summary of activity-based similarities with other entities performing similar actions over time.",  
        "attribute_mapping": {
            "attribute_counts": {
                "title": "", 
                "intro": "",  
                "type": AttributeType.COUNT_PERCENT_RANK.value,
                "format": FormatType.TABLE.value,
                "columns": percent_rank_columns,
                "data": [],
            }
        },
    },
    "own_redflag_details": {
        "title": "Own review flag details",
        "intro": "Details of the review flags associated with the target entity.",
        "attribute_mapping": {
            "attribute_values": {
                "title": "Attribute Values",
                "intro": "",  # TODO: TBD for Darren or Ha
                "type": AttributeType.LIST.value,
                "format": FormatType.TABLE.value,
                "columns": flag_columns,
                "data": [],
            }
        },
    },
    "direct_redflag_details": {
        "title": "Directly-related entities with review flags",
        "intro": "Details of entities that are directly related to the target entity and which are associated with at least one review flag.",
       
        "attribute_mapping": {
            "attribute_values": {
                "title": "",  
                "intro": "", 
                "type": AttributeType.LIST.value,
                "format": FormatType.COMPLEX_TABLE.value,
                "columns": flag_details_columns,
                "data": [],
            }
        },
    },
    "indirect_redflag_details": {
        "title": "Indirectly-related entities with review flags",
        "intro": "The following are entities that are indirectly related to the target entity and carry at least one review flag",
        "attribute_mapping": {
            "attribute_values": {
                "title": "", 
                "intro": "", 
                "type": AttributeType.FLAGS.value,
                "format": FormatType.COMPLEX_TABLE.value,
                "columns": flag_details_columns,
                "data": [],
            }
        },
    },
    "sync_entities": {
        "title": "Entities with synchronous activity similarity",
        "intro": "The following are entities that have similar activity with the target entity (i.e., performing the same actions) in the same periods.",
        "attribute_mapping": {
            "attribute_counts": {
                "title": "Summary of shared activity",
                "intro": "", 
                "type": AttributeType.COUNT_PERCENT.value,
                "format": FormatType.TABLE.value,
                "columns": attribute_values_columns,
                "data": [],
            },

            "attribute_values": {
                "title": "", 
                "intro": "", 
                "type": AttributeType.FLAGS.value,
                "columns": flag_details_columns,
                "format": FormatType.COMPLEX_TABLE.value,
                "data": [],
            },
            
            "attribute_charts": {
                "title": "",  
                "intro": "",
                "type": AttributeType.LIST.value,
                "format": FormatType.CHART.value,
                "data": [],
            },
        },
    },
 
    "async_entities": {
        "title": "Entities with asynchronous activity similarity",

        "intro": "The following are entities that have similar activity with the target entity (i.e., performing the same actions) in different periods.",
        "attribute_mapping": {
            "attribute_counts": {
                "title": "Summary of shared activity",
                "intro": "", 
                "type": AttributeType.COUNT_PERCENT.value,
                "format": FormatType.TABLE.value,
                "columns": attribute_values_columns,
                "data": [],
            },

            "attribute_values": {
                "title": "",
                "intro": "",
                "type": AttributeType.LIST.value,
                "columns": flag_details_columns,
                "format": FormatType.COMPLEX_TABLE.value,
                "data": [],
            },
            
            "attribute_charts": {
                "title": "",
                "intro": "", 
                "type": AttributeType.LIST.value,
                "format": FormatType.CHART.value,
                "data": [],
            },
        },
    },
}