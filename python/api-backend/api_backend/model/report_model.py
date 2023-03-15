#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

RawSection = Dict[str, Any]


@dataclass
class RawReport(Dict):
    entity_info_details: Optional[RawSection] = None
    risk_summary_details: Optional[RawSection] = None
    activity_summary_details: Optional[RawSection] = None
    direct_redflag_details: Optional[RawSection] = None
    indirect_redflag_details: Optional[RawSection] = None
    sync_entities: Optional[RawSection] = None
    async_entities: Optional[RawSection] = None
    own_red_flags_details: Optional[RawSection] = None


class AttributeType(str, Enum):
    COUNT = "count"
    COUNT_PERCENT = "count-percent"
    COUNT_PERCENT_RANK = "count-percent-rank"
    LIST = "list"
    FLAGS = "flags"


class FormatType(str, Enum):
    TABLE = "table"
    COMPLEX_TABLE = "complex-table"
    CHART = "chart"


@dataclass
class RawReviewFlag(Dict):
    description: str
    details: List[Dict[str, Union[str, int]]]


@dataclass
class ReviewFlag(Dict):
    flag: str
    evidence: List[Dict[str, Union[str, int]]]

    def to_dict(self):
        return {
            "flag": self.flag,
            "evidence": self.evidence,
        }


@dataclass
class RelationshipPaths(Dict):
    direct_paths: List[str]
    indirect_paths: int

    def to_dict(self):
        return {
            "direct_paths": self.direct_paths,
            "indirect_paths": self.indirect_paths,
        }


# Used for both direct and synchronous and asynchronous activity analysis attribute counts
@dataclass
class ActivityAnalysisCounts:
    key: str
    value: List[Dict[str, float]]

    def to_dict(self):
        return {
            "key": self.key,
            "value": self.value,
        }


@dataclass
class EntityActivity:
    time: str
    attribute: str
    value: str

    def to_dict(self):
        return {
            "time": self.time,
            "attribute": self.attribute,
            "value": self.value,
        }


TargetEntityOverviewCounts = List[Union[str, int]]
TargetEntityOverviewValues = List[Union[str, List[str]]]

# Used for both review flag and activity summary
CountPercentRank = List[Union[str, int, float]]

# Used for both direct and indirect related entities and for both synchronous and asynchronous activity analysis attribute values
@dataclass
class RelatedEntityActivity:
    entity_id: str
    relationship_paths: RelationshipPaths
    review_flags: List[ReviewFlag]

    def to_list(self):
        return [
            self.entity_id,
            self.relationship_paths.to_dict(),
            self.review_flags,
        ]


Data = TypeVar("Data")


@dataclass
class Attribute(Generic[Data]):
    data: List[Data]
    title: Optional[str] = ""
    intro: Optional[str] = ""
    type: Optional[AttributeType] = None
    format: Optional[FormatType] = None
    columns: Optional[List[str]] = None

    def to_dict(self):
        return {
            "title": self.title,
            "intro": self.intro,
            "type": self.type,
            "format": self.format,
            "columns": self.columns,
            "data": self.data,
        }


@dataclass
class AttributeMapping(Dict):
    attribute_counts: Optional[Attribute] = Attribute(data=[])
    attribute_values: Optional[Attribute] = Attribute(data=[])
    attribute_charts: Optional[Attribute] = Attribute(data=[])

    def to_dict(self):
        return {
            "attribute_counts": self.attribute_counts.to_dict() if self.attribute_counts.data else None,
            "attribute_values": self.attribute_values.to_dict() if self.attribute_values.data else None,
            "attribute_charts": self.attribute_charts.to_dict() if self.attribute_charts.data else None,
        }


@dataclass
class Section(Dict):
    attribute_mapping: AttributeMapping
    id: Optional[str] = ""
    title: Optional[str] = ""
    intro: Optional[str] = ""

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "intro": self.intro,
            "attribute_mapping": self.attribute_mapping.to_dict(),
        }


@dataclass
class Report:
    html_report: Dict[str, List[Section]]

    def to_dict(self):
        return {
            "html_report": self.html_report,
        }
