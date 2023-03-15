#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
"""Contains the base class for transformers that transform a Spark dataframe to a new dataframe."""

from abc import ABC, abstractmethod
from typing import Any, Tuple, Union

from pyspark.sql import DataFrame


class BaseLinkFilter(ABC):
    """Base class for a filter that transforms a Spark dataframe to a new dataframe."""

    def __init__(self, config: Any):
        """Initialize a filter."""
        self.config = config

    @abstractmethod
    def filter(
        self, input_data: DataFrame
    ) -> Union[DataFrame, Tuple[DataFrame, DataFrame]]:
        """
        Perform the gilters on the input dataframe and return a new dataframe.

        Params:
            input_data: Spark DataFrame The input dataset to apply filters

        Returns:
            output_data: Spark DataFrame The output of the filtering
        """
        pass
