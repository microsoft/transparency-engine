#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
"""Contains the base class for transformers that transform a Spark dataframe to a new dataframe."""

import logging

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Base class for a transformer that transforms a Spark dataframe to a new dataframe."""

    def __init__(self, configs: Any):
        """Initialize a transformer."""
        self.configs = configs

    @abstractmethod
    def transform(self, input_data: DataFrame) -> DataFrame:
        """
        Perform the transformations on the input dataframe and return a new dataframe.

        Params:
            input_data: Spark DataFrame The input dataset to apply transformations

        Returns:
            output_data: Spark DataFrame The output of the transformation
        """
        pass
