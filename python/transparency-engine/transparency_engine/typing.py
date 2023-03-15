#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

"""
Typing Module for Transparency Engine.

Includes common types and enums used across different stages of the pipeline.
"""

from enum import Enum
from typing import List

from pyspark.sql.types import StructType

from transparency_engine.pipeline.schemas import (
    ATTRIBUTE_SCHEMA,
    DYNAMIC_SCHEMA,
    ENTITY_SCHEMA,
    METADATA_SCHEMA,
    REVIEW_FLAG_METADATA_SCHEMA,
    REVIEW_FLAG_SCHEMA,
    STATIC_SCHEMA,
)


class PipelineSteps(str, Enum):
    """
    Pipeline steps.

    This class contains the stages for the pipeline.
    """

    DATA_PREP = "prep"
    IND_LINK_PREDICTION = "individual_link_prediction"
    IND_LINK_FILTERING = "individual_link_filtering"
    MACRO_LINK_PREDICTION = "macro_link_prediction"
    MACRO_LINK_FILTERING = "macro_link_filtering"
    SCORING = "scoring"
    REPORT = "report"
    ALL = "all"

    @staticmethod
    def from_string(step: str) -> "PipelineSteps":
        """
        Get the pipeline step from a string.

        Parameters
        ----------
        step : str
            The pipeline step as a string.
        Returns
        -------
        PipelineSteps
            The pipeline step.
        """
        if step == "prep":
            return PipelineSteps.DATA_PREP
        elif step == "individual_link_prediction":
            return PipelineSteps.IND_LINK_PREDICTION
        elif step == "individual_link_filtering":
            return PipelineSteps.IND_LINK_FILTERING
        elif step == "macro_link_prediction":
            return PipelineSteps.MACRO_LINK_PREDICTION
        elif step == "macro_link_filtering":
            return PipelineSteps.MACRO_LINK_FILTERING
        elif step == "scoring":
            return PipelineSteps.SCORING
        elif step == "report":
            return PipelineSteps.REPORT
        elif step == "all":
            return PipelineSteps.ALL
        else:
            raise ValueError(f"Unknown step: {step}")

    @staticmethod
    def from_string_list(steps: List[str]) -> List["PipelineSteps"]:
        """
        Get the pipeline step from a string.

        Parameters
        ----------
        step : str
            The pipeline step as a string.
        Returns
        -------
        PipelineSteps
            The pipeline step.
        """
        return [PipelineSteps.from_string(step) for step in steps]


class InputLoadTypes(Enum):
    """
    Input load types.

    This class contains the input load types for the pipeline.
    """

    Static = "static", STATIC_SCHEMA
    Dynamic = "dynamic", DYNAMIC_SCHEMA
    Metadata = "metadata", METADATA_SCHEMA
    Attribute = "attribute", ATTRIBUTE_SCHEMA
    ReviewFlag = "reviewflag", REVIEW_FLAG_SCHEMA
    ReviewFlagMetadata = "reviewflagmetadata", REVIEW_FLAG_METADATA_SCHEMA
    Entity = "entity", ENTITY_SCHEMA

    @property
    def name(self) -> str:
        """
        Get the Input Load type name.

        Returns
            name: str
            The input type name. E.G: static
        """
        return self.value[0]

    @property
    def schema(self) -> StructType:
        """
        Get the Input Load type schema.

        Returns
            schema: StructType
            The input type schema.
        """
        return self.value[1]

    @staticmethod
    def from_string(input_type: str) -> "InputLoadTypes":
        """
        Get the input type from a string.

        Parameters
        ----------
        input_type : str
            The input type as a string.
        Returns
        -------
        InputType
            The input type.
        """
        if input_type == "static":
            return InputLoadTypes.Static
        elif input_type == "dynamic":
            return InputLoadTypes.Dynamic
        elif input_type == "metadata":
            return InputLoadTypes.Metadata
        elif input_type == "reviewflagmetadata":
            return InputLoadTypes.ReviewFlagMetadata
        elif input_type == "attribute":
            return InputLoadTypes.Attribute
        elif input_type == "reviewflag":
            return InputLoadTypes.ReviewFlag
        elif input_type == "entity":
            return InputLoadTypes.Entity
        else:
            raise ValueError(f"Unknown input type: {input_type}")
