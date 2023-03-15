#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import json

from api_backend.report.util.util import title_to_key


def parse_entity_details(entity_results):
    entity_details = {}
    for row in entity_results:
        attribute = row[1]
        value = row[2]
        values = entity_details.get(attribute, [])
        if value not in values:
            values.append(value)
            entity_details[attribute] = values
    return entity_details


def parse_raw_report(column_names, report_results):
    raw_report = {}
    for row in report_results:
        for key, value in zip(column_names, row):
            formatted_key = title_to_key(key)
            if value is not None:
                raw_report[formatted_key] = parse_value(value)
            else:
                raw_report[formatted_key] = value
    return raw_report


def parse_filtered_activity_attributes(filtered_activity_attributes_results, target_id):
    filtered_activity_attributes = {}
    for item in filtered_activity_attributes_results:
        entity = item[0].split("::")[1]
        time = item[1]
        attribute = item[2]
        value = item[3]
        if entity not in filtered_activity_attributes:
            filtered_activity_attributes[entity] = {}
        values = filtered_activity_attributes[entity].get("values", [])
        values.append({"time": time, "attribute": attribute, "value": value})
        filtered_activity_attributes[entity]["values"] = values
        filtered_activity_attributes[entity]["is_target"] = entity == target_id
    return filtered_activity_attributes


def parse_value(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except:
            pass

    if isinstance(value, list):
        # Recursively process each element of the list
        return [parse_value(item) for item in value]
    elif isinstance(value, dict):
        # Recursively process each value of the dictionary
        return {title_to_key(key): parse_value(value) for key, value in value.items()}
    else:
        # Try convert the result to a JSON object, otherwise return the value
        try:
            return json.loads(value)
        except:
            return value
