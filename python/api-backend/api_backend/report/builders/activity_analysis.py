#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


from api_backend.report.util.util import build_related_entities_data


def get_attribute_counts(raw_section):
    data = {}
    for item in raw_section:
        entity_id = item["related"]
        for key in item:
            new_key = key.split("_")[0]
            value = data.get(new_key, [])
            new_value = {
                "entity_id": entity_id,
                "value": item[key],
            }
            value.append(new_value)
            data[new_key] = value
    return [{"key": key, "value": value} for key, value in data.items()]


def get_attribute_values(raw_section):
    return build_related_entities_data(raw_section)


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
