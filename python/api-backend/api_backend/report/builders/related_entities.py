#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


from api_backend.report.util.util import build_related_entities_data


def get_attribute_values(raw_section):
    return build_related_entities_data(raw_section)
