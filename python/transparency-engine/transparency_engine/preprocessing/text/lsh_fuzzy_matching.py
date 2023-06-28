#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from functools import reduce
from typing import Dict

import pyspark.sql.functions as F

from dataclasses import dataclass
from pyspark.sql import DataFrame

from transparency_engine.base_transformer import BaseTransformer
from transparency_engine.modules.text import lsh_match_text


logger = logging.getLogger(__name__)


@dataclass
class LSHConfig:
    data_partitions: int = 640
    include_word_delimiter: bool = True
    ngram_length: int = 4
    num_hash_tables: int = 3
    min_df: float = 0.0001
    max_df: float = 0.1
    min_similarity: float = 0.8


class LSHFuzzyMatchTransformer(BaseTransformer):
    """
    Transformer that performs fuzzy matching on multiple text attribute types in a dataframe using LSH algorithm on ngrams
    """

    def __init__(
        self,
        configs: Dict[str, LSHConfig],
        attribute_col: str = "AttributeID",
        value_col: str = "Value",
    ):
        """
        Params:
            configs: dict
                Contains fuzzy matching configurations for each text attribute
            attribute_col: str, default = 'AttributeID'
                Name of the column that contains different types of text attribute (e.g. name, address)
            value_col: str, default = 'Value'
                Text value of attributes
        """
        super().__init__(configs)
        self.attribute_col = attribute_col
        self.value_col = value_col

    def transform(self, input_data: DataFrame) -> DataFrame:
        """
        Perform fuzzy matching usng LSH algo on ngrams for each type of text attribute in the input dataframe

        Params:
            input_data: Spark DataFrame
                Entity attribute data with schema [entity_col, attribute_col, value_col]

        Returns:
            Spark DataFrame
                Fuzzy matching results with schema [Source, Target, Similarity, AttributeID]
        """

        results = []
        for attribute in self.configs:
            attribute_data = (
                input_data.filter(F.col(self.attribute_col) == attribute)
                .select(self.value_col)
                .dropDuplicates()
            )

            # get fuzzy matching params
            attribute_configs = self.configs[attribute]
            data_partitions = attribute_configs.data_partitions
            include_word_delimiter = attribute_configs.include_word_delimiter
            ngram_length = attribute_configs.ngram_length
            num_hash_tables = attribute_configs.num_hash_tables
            min_df = attribute_configs.min_df
            max_df = attribute_configs.max_df
            min_similarity = attribute_configs.min_similarity

            fuzzy_match_data = lsh_match_text(
                data=attribute_data,
                text_col=self.value_col,
                data_partitions=data_partitions,
                include_word_delimiter=include_word_delimiter,
                ngram_length=ngram_length,
                num_hash_tables=num_hash_tables,
                min_df=min_df,
                max_df=max_df,
                min_similarity=min_similarity,
            )
            fuzzy_match_data = fuzzy_match_data.withColumn(
                self.attribute_col, F.lit(attribute)
            ).cache()
            logger.info(f"Matching records for {attribute}: {fuzzy_match_data.count()}")
            results.append(fuzzy_match_data)
        return reduce(DataFrame.unionAll, results)
