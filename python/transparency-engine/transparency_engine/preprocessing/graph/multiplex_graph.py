#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from functools import reduce
from typing import Dict, Union

from dataclasses import dataclass
from pyspark.sql import DataFrame

import transparency_engine.modules.graph.preprocessing.graph_edges as graph_edges

from transparency_engine.base_transformer import BaseTransformer


logger = logging.getLogger(__name__)


@dataclass
class MultiplexGraphConfig:
    exact_match_weight: float = 1.0
    use_jaccard_weight: bool = True


class MultiplexGraphTransformer(BaseTransformer):
    """
    Transformer that converts a static relationship data frame (e.g. entity contact data) into an edge list of a multiplex graph.
    """

    def __init__(
        self,
        configs: Dict[str, MultiplexGraphConfig],
        fuzzy_match_data: Union[DataFrame, None] = None,
        entity_col: str = "EntityID",
        attribute_col: str = "AttributeID",
        value_col: str = "Value",
    ):
        """
        Params:
            configs: dict
                Contains multiplex graph configuration for each text attribute, e.g
            fuzzy_match_data: DataFrame
                Contains fuzzy matching results with schema [Source, Target, fuzzy_match_weight_col, attribute_col]
            entity_col: str, default = 'EntityID'
                Name of the column that contains entity ids in the input dataframe
            attribute_col: str, default = 'AttributeID'
                Name of the column that contains different types of text attribute (e.g. name, address)
            value_col: str, default = 'Value'
                Name of column that contain attribute values
        """
        super().__init__(configs)
        self.fuzzy_match_data = fuzzy_match_data
        self.entity_col = entity_col
        self.attribute_col = attribute_col
        self.value_col = value_col

    def transform(self, input_data: DataFrame) -> DataFrame:
        """
        Convert static relationship input data into graph edges of a multiplex graph,
        taking into account both exact matching and fuzzy matching.

        Params:
            input_data: Spark DataFrame
                static relationship data with schema [entity_col, attribute_col, value_col]

        Returns:
            Spark DataFrame Graph edge data with schema [Source, Target, SourceType, TargetType, Relationship, Weight]
        """
        results = []

        for attribute, attribute_configs in self.configs.items():
            # get graph edge configs
            exact_match_weight = attribute_configs.exact_match_weight
            use_jaccard_weight = attribute_configs.use_jaccard_weight

            graph = graph_edges.generate_unipartite_edges(
                entity_attribute_data=input_data,
                fuzzy_match_data=self.fuzzy_match_data,
                attribute=attribute,
                entity_col=self.entity_col,
                attribute_col=self.attribute_col,
                value_col=self.value_col,
                exact_match_weight=exact_match_weight,
                use_jaccard_weight=use_jaccard_weight,
            ).cache()
            logger.info(f"Multiplex graph edges for {attribute}: {graph.count()}")
            results.append(graph)
        return reduce(DataFrame.unionAll, results)
