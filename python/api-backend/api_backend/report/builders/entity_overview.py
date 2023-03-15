#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from api_backend.report.util.util import key_to_title


def get_attribute_counts(raw_section):
    data = []
    for key in raw_section:
        if isinstance(raw_section[key], int):
            data.append([key_to_title(key), raw_section[key]])
    return data


def get_attribute_values(entity_details):
    data = []
    for key in entity_details:
        data.append([key_to_title(key), entity_details[key]])
    return data
