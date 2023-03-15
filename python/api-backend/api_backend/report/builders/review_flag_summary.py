#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from api_backend.report.util.util import build_count_percent_rank_data


def get_attribute_counts(raw_section):
    return build_count_percent_rank_data(raw_section)
