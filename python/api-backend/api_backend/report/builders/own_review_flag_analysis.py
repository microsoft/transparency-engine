#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from api_backend.report.util.util import get_review_flags


def get_attribute_values(raw_section):
    return get_review_flags(raw_section)
