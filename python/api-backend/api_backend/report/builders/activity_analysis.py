#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


from api_backend.report.util.util import build_related_entities_data


def get_attribute_counts(raw_section):
    data = {}
    for item in raw_section:
        entity_id = item["related"].split("::")[1]
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


def get_attribute_charts(filtered_activity_attributes):
    target_entity_activity = []
    related_entity_activity = []
    if filtered_activity_attributes:
        for key in filtered_activity_attributes:
            values = filtered_activity_attributes[key].get("values", [])
            if filtered_activity_attributes[key]["is_target"]:
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
