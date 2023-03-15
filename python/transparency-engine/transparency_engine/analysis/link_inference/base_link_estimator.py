#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from abc import abstractmethod
from typing import Generic, List, TypeVar, Union

from dataclasses import dataclass
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


@dataclass
class BaseLinkConfig:
    """
    Base config for an estimator that predicts links between entities based on static attributes.

    Params:
        n_links: int, default = 20
            Max number of links to return for each entity
    """

    n_links: int = 20


LinkConfig = TypeVar("LinkConfig", bound=BaseLinkConfig)


class BaseLinkEstimator(Generic[LinkConfig]):
    """
    An estimator that predicts links between entities based on static or dynamic relationships.
    """

    def __init__(
        self,
        configs: LinkConfig,
    ):
        """
        Params:
            configs:  LinkConfig
                Configurations for the estimator that predicts entity links
        """
        self.configs = configs

    @abstractmethod
    def predict(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        """
        Predict entity links based on entities' static/dynamic relationships.

        Params:
            input_data: Spark DataFrame
                Contains data related to entity relationships

        Returns:
            Spark DataFrame Dataframe containing predicted links
        """
        pass
