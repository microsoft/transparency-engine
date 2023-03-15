#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import numpy as np
import pyspark.sql.functions as F

from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType
from scipy.spatial.distance import cosine


def cosine_similarity(source_embedding, target_embedding):
    """
    Computes the cosine similarity between two lists of numbers

    Parameters:
    source_embedding (list): first list of numbers
    target_embedding (list): second list of numbers

    Returns:
    float: cosine similarity between the two lists
    """
    if source_embedding is not None and target_embedding is not None:
        return float(1 - cosine(np.array(source_embedding), np.array(target_embedding)))
    else:
        return None


def multiple_set_intersection_size(set_list: list):
    """
    Computes the size of intersection of multiple sets

    Parameters:
    set_list (list): list of sets

    Returns:
    int: size of intersection of the sets
    """
    cleaned_set_list = [set(s) for s in set_list if len(s) > 0]
    if len(cleaned_set_list) > 1:
        return len(set.intersection(*cleaned_set_list))
    else:
        return 0


def compute_jaccard_similarity(list1: list, list2: list) -> float:
    """
    Computes Jaccard Similarity between two lists

    Parameters:
    list1 (list): first list
    list2 (list): second list

    Returns:
    float: Jaccard similarity between the two lists
    """
    intersection = len(set(list1).intersection(set(list2)))
    union = len(set(list1).union(set(list2)))
    if union == 0:
        return 0.0
    else:
        return intersection / union


def compute_overlap_coefficient(list1: list, list2: list):
    """
    Computes Overlap Coefficient between two lists

    Parameters:
    list1 (list): first list
    list2 (list): second list

    Returns:
    float: Overlap Coefficient between the two lists
    """
    setlist1 = set(list1)
    setlist2 = set(list2)
    intersection = len(setlist1.intersection(setlist2))
    min_size = min(len(setlist1), len(setlist2))
    if min_size == 0:
        return 0.0
    else:
        return intersection / min_size


# UDFS

cosine_similarity_udf = F.udf(cosine_similarity, FloatType())

set_intersection_udf = F.udf(
    lambda list1, list2: list(set(list1).intersection(set(list2))),
    ArrayType(StringType()),
)
set_intersection_size_udf = F.udf(
    lambda list1, list2: len(set(list1).intersection(set(list2))), IntegerType()
)
set_union_udf = F.udf(
    lambda list1, list2: list(set(list1).union(set(list2))), ArrayType(StringType())
)
set_union_size_udf = F.udf(
    lambda list1, list2: len(set(list1).union(set(list2))), IntegerType()
)
multiple_set_union_size_udf = F.udf(
    lambda set_list: len(set().union(*set_list)), IntegerType()
)
set_difference_udf = F.udf(
    lambda list1, list2: list(set(list1).difference(set(list2))),
    ArrayType(StringType()),
)
set_difference_size_udf = F.udf(
    lambda list1, list2: len(set(list1).difference(set(list2))), IntegerType()
)
fill_empty_array_udf = F.udf(
    lambda attribute_list: list() if attribute_list is None else attribute_list,
    ArrayType(StringType()),
)
multiple_set_intersection_size_udf = F.udf(
    multiple_set_intersection_size, IntegerType()
)
jaccard_similarity_udf = F.udf(compute_jaccard_similarity, FloatType())
overlap_coefficient_udf = F.udf(compute_overlap_coefficient, FloatType())
