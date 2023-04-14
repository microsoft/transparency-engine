#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


from api_backend.report.constants.attributes import link_summary, related
from api_backend.report.util.util import build_activity_values_data


def get_attribute_counts(raw_section):
    data = {}

    if raw_section is None:
        return []

    for item in raw_section:
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


def get_attribute_values(raw_section):
    return build_activity_values_data(raw_section)


def get_attribute_charts(activity):
    target_entity_activity = []
    related_entity_activity = []
    if activity:
        for key in activity:
            values = activity[key].get("values", [])
            if activity[key]["is_target"]:
                target_entity_activity = values
            else:
                related_entity_activity.append({"entity_id": key, "activity": values})

    return [
        {
            "key": "target entity activity",
            "value": target_entity_activity,
        },
        {
            "key": "related entity activity",
            "value": related_entity_activity,
        },
    ]
