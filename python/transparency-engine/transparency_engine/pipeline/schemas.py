#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from typing import Final

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Columns
ENTITY_ID: Final[str] = "EntityID"
ENTITY_WEIGHT: Final[str] = "EntityWeight"
ATTRIBUTE_ID: Final[str] = "AttributeID"
VALUE: Final[str] = "Value"
TIME_PERIOD: Final[str] = "TimePeriod"
NAME: Final[str] = "Name"
DESCRIPTION: Final[str] = "Description"
REVIEW_FLAG_ID: Final[str] = "FlagID"
CATEGORY: Final[str] = "Category"
IS_SUPPORTING_FLAG: Final[str] = "IsSupportingFlag"
EVIDENCE: Final[str] = "Evidence"
REVIEW_FLAG_WEIGHT: Final[str] = "FlagWeight"
SOURCE_NODE: Final[str] = "Source"
TARGET_NODE: Final[str] = "Target"
PATHS: Final[str] = "Paths"
WEIGHT: Final[str] = "Weight"

SOURCE: Final[str] = "Source"
TARGET: Final[str] = "Target"
SOURCE_TYPE: Final[str] = "SourceType"
TARGET_TYPE: Final[str] = "TargetType"
RELATED: Final[str] = "Related"
RELATIONSHIP: Final[str] = "Relationship"
ACTIVITIES: Final[str] = "Activities"
DYNAMIC_LINK_TYPE: Final[str] = "DynamicLinkType"

SYNC_ACTIVITY_PREFIX: Final[str] = "sync_"
ASYNC_ACTIVITY_PREFIX: Final[str] = "async_"

NORMALIZED_JACCARD_SIMILARITY: Final[str] = "NormalizedJaccardSimilarity"
NORMALIZED_JACCARD_ANOMALY: Final[str] = "NormalizedJaccardAnomaly"
JACCARD_SIMILARITY: Final[str] = "JaccardSimilarity"
JACCARD_ANOMALY: Final[str] = "JaccardAnomaly"
OVERLAP_SCORE: Final[str] = "OverlapScore"
SHARED: Final[str] = "Shared"
ACTIVITY_ATTRIBUTE_TYPE: Final[str] = "type"


# Schemas
ENTITY_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ENTITY_ID, StringType(), False),
        StructField(ENTITY_WEIGHT, FloatType(), True),
    ]
)

DYNAMIC_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ENTITY_ID, StringType(), False),
        StructField(ATTRIBUTE_ID, StringType(), False),
        StructField(VALUE, StringType(), True),
        StructField(TIME_PERIOD, StringType(), True),
    ]
)

STATIC_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ENTITY_ID, StringType(), False),
        StructField(ATTRIBUTE_ID, StringType(), False),
        StructField(VALUE, StringType(), True),
    ]
)

METADATA_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ATTRIBUTE_ID, StringType(), False),
        StructField(NAME, StringType(), True),
        StructField(DESCRIPTION, StringType(), True),
    ]
)

REVIEW_FLAG_METADATA_SCHEMA: Final[StructType] = StructType(
    [
        StructField(REVIEW_FLAG_ID, StringType(), False),
        StructField(CATEGORY, StringType(), True),
        StructField(NAME, StringType(), True),
        StructField(DESCRIPTION, StringType(), True),
        StructField(IS_SUPPORTING_FLAG, BooleanType(), False),
        StructField(REVIEW_FLAG_WEIGHT, FloatType(), False),
    ]
)

ATTRIBUTE_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ENTITY_ID, StringType(), False),
        StructField(ATTRIBUTE_ID, StringType(), False),
        StructField(VALUE, StringType(), True),
    ]
)

REVIEW_FLAG_SCHEMA: Final[StructType] = StructType(
    [
        StructField(ENTITY_ID, StringType(), False),
        StructField(REVIEW_FLAG_ID, StringType(), False),
        StructField(EVIDENCE, StringType(), True),
    ]
)

PREDICTED_LINK_SCHEMA: Final[StructType] = StructType(
    [
        StructField(SOURCE_NODE, StringType(), False),
        StructField(TARGET_NODE, StringType(), False),
        StructField(PATHS, ArrayType(ArrayType(StringType())), False),
    ]
)

ENTITY_TEMPORAL_ACTIVITY_SCHEMA: Final[StructType] = StructType(
    [
        StructField(SOURCE, StringType(), False),
        StructField(TARGET, StringType(), False),
        StructField(ACTIVITY_ATTRIBUTE_TYPE, StringType(), False),
        StructField(TIME_PERIOD, StringType(), False),
        StructField(SHARED, IntegerType(), False),
        StructField(f"{SOURCE}_only", IntegerType(), False),
        StructField(f"{TARGET}_only", IntegerType(), False),
        StructField(JACCARD_SIMILARITY, FloatType(), False),
        StructField(OVERLAP_SCORE, FloatType(), False),
    ]
)
