#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import re
from typing import Dict, List, Optional

from api_backend.model.report_model import (
    CountPercentRank,
    RawReviewFlag,
    RawSection,
    RelatedEntityActivity,
    RelationshipPaths,
    ReviewFlag,
)
from api_backend.report.constants.attributes import (
    direct_links,
    evidence,
    flag,
    indirect_link_count,
    link_summary,
    related,
    related_flag_details,
)


def key_to_title(key: str):
    return key.replace("_", " ")


def title_to_key(key: str):
    key = key.strip().lower()
    key = re.sub(r"[-\s]+", "_", key)
    key = re.sub(r"[()]+", "", key)
    return key


def format_key(key, mapping=None):
    if mapping is not None and key in mapping:
        return mapping[key]
    else:
        return key_to_title(key).replace("companies", "entities").replace("red ", "review ")


def build_count_percent_rank_data(
    report_section: RawSection, attribute_mapping: Optional[Dict[str, str]] = None
) -> List[CountPercentRank]:
    data: List[CountPercentRank] = []

    if report_section is None:
        return data

    for key in report_section:
        pct_rank = report_section.get(key + "_pct", None)
        if not "_pct" in key and pct_rank and (attribute_mapping is None or key in attribute_mapping):
            count = report_section[key]
            key = format_key(key, attribute_mapping)
            pct_value = 0 if count == 0 else (100 - round(pct_rank, ndigits=2))
            item = [key, count, pct_value]
            data.append(item)
    return data


def get_relationship_paths(item) -> RelationshipPaths:
    relationship_paths = RelationshipPaths(direct_paths=[], indirect_paths=0)
    if link_summary in item:
        relationship_paths = RelationshipPaths(
            direct_paths=item[link_summary].get(direct_links, []),
            indirect_paths=item[link_summary].get(indirect_link_count, 0),
        )
    return relationship_paths


def build_related_entities_data(report_section: List[RawSection]) -> List[RelatedEntityActivity]:
    data: List[RelatedEntityActivity] = []

    if report_section is None:
        return data

    for item in report_section:
        entity_id = item[related]
        relationship_paths = get_relationship_paths(item)
        review_flags: List[ReviewFlag] = []
        if related_flag_details in item:
            review_flags = get_review_flags(item[related_flag_details])
        data_item = RelatedEntityActivity(
            entity_id=entity_id, relationship_paths=relationship_paths, review_flags=review_flags
        )
        data.append(data_item.to_list())

    return data


def get_review_flags(review_flag_details: List[RawReviewFlag]) -> List[ReviewFlag]:
    review_flags: List[ReviewFlag] = []
    if review_flag_details is None:
        return review_flags
    for rf in review_flag_details:
        review_flag = ReviewFlag(
            flag=rf.get(flag, ""),
            evidence=rf.get(evidence, ""),
        )
        review_flags.append(review_flag.to_dict())
    return review_flags


def get_measurements(report_section):
    data = {}

    if report_section is None:
        return []

    for item in report_section:
        entity_id = item[related]
        for key in item:
            if key == related or key == link_summary or "average" in key or key == "flag_count":
                continue
            new_key = key.split("_")[0] if "score" in key else key.replace("_", " ")
            value = data.get(new_key, [])
            new_value = {
                "entity_id": entity_id,
                "value": round(item[key] * 100, 2),
            }
            value.append(new_value)
            data[new_key] = value
    return [{"key": key, "value": value} for key, value in data.items()]


def build_activity_values_data(report_section: List[RawSection]):
    data = []

    if report_section is None:
        return data

    for item in report_section:
        entity_id = item[related]
        relationship_paths = get_relationship_paths(item)
        measurements = get_measurements(report_section)
        data_item = [entity_id, relationship_paths.to_dict(), measurements]
        data.append(data_item)

    return data
