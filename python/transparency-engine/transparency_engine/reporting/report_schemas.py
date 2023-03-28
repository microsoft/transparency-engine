#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

"""
This module contains names of data fields and table columns of the output data
used to populate the final report.
"""

from typing import Final, List

from transparency_engine.analysis.scoring.measures import NetworkMeasures


# Default entity name attribute
DEFAULT_ENTITY_NAME_ATTRIBUTE: Final[str] = "default_name"

# activity link type values (to be used in PowerBI report)
LINK_TYPE: Final[str] = "link_type"
SYNC_ACTIVITY_LINK_TYPE: Final[str] = "sync_activity"
ASYNC_ACTIVITY_LINK_TYPE: Final[str] = "async_activity"

# Flag summary data keys
FLAG_SUMMARY_MEASURES: Final[List[NetworkMeasures]] = [
    NetworkMeasures.NETWORK_FLAG_COUNT,
    NetworkMeasures.NETWORK_FLAG_WEIGHT,
    NetworkMeasures.TARGET_FLAG_COUNT,
    NetworkMeasures.TARGET_FLAG_WEIGHT,
    NetworkMeasures.RELATED_FLAG_COUNT,
    NetworkMeasures.RELATED_FLAG_WEIGHT,
    NetworkMeasures.RELATED_FLAGGED_COUNT,
    NetworkMeasures.DIRECT_FLAG_COUNT,
    NetworkMeasures.DIRECT_FLAG_WEIGHT,
    NetworkMeasures.DIRECT_FLAGGED_COUNT,
    NetworkMeasures.INDIRECT_FLAG_COUNT,
    NetworkMeasures.INDIRECT_FLAG_WEIGHT,
    NetworkMeasures.INDIRECT_FLAGGED_COUNT,
]
PERCENT_RANK_MEASURE_POSTFIX: Final[str] = "pct"

# Flag details data keys
FLAG_DESCRIPTION: Final[str] = "flag"
FLAG_EVIDENCE: Final[str] = "evidence"

# Related entity data keys
RELATED_ENTITY: Final[str] = "related"
LINK_SUMMARY: Final[str] = "link_summary"
DIRECT_LINKS: Final[str] = "direct_links"
DIRECT_LINK_COUNT: Final[str] = "direct_link_count"
INDIRECT_LINK_COUNT: Final[str] = "indirect_link_count"
RELATED_FLAG_DETAILS: Final[str] = "related_flag_details"

# Activity summary data keys
SYNC_LINK_COUNT: Final[str] = "sync_link_count"
ASYNC_LINK_COUNT: Final[str] = "async_link_count"
AVERAGE_SCORE: Final[str] = "average_score"
PERIOD_SCORE: Final[str] = "period_score"
ACTIVITY_FLAG_COUNT: Final[str] = "flag_count"

# Final report columns
ATTRIBUTE_SUMMARY: Final[str] = "AttributeSummary"
REVIEW_FLAG_SUMMARY: Final[str] = "FlagSummary"
ACTIVITY_SUMMARY: Final[str] = "ActivitySummary"
OWN_FLAGS: Final[str] = "OwnFlags"
DIRECT_FLAGS: Final[str] = "DirectFlags"
INDIRECT_FLAGS: Final[str] = "IndirectFlags"
SYNC_ACTIVITY: Final[str] = "SyncActivity"
ASYNC_ACTIVITY: Final[str] = "AsyncActivity"


# graph edge columns in the final entity graph output table
PATH_SOURCE_NODE: Final[str] = "PathSource"
PATH_TARGET_NODE: Final[str] = "PathTarget"
PATH_SOURCE_TYPE: Final[str] = "PathSourceType"
PATH_TARGET_TYPE: Final[str] = "PathTargetType"
PATH_SOURCE_RELATIONSHIP: Final[str] = "PathSourceRelationship"
PATH_TARGET_RELATIONSHIP: Final[str] = "PathTargetRelationship"
PATH_SOURCE_FLAG: Final[str] = "PathSourceFlag"
PATH_TARGET_FLAG: Final[str] = "PathTargetFlag"
