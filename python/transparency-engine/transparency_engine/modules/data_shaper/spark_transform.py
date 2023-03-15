#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from itertools import chain
from typing import List, Union

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

from transparency_engine.pipeline.schemas import ATTRIBUTE_ID, VALUE


"""
This module contains Spark utility functions for transforming Spark dataframes.
"""


def melt(
    data: DataFrame,
    id_cols: Union[str, List[str]],
    value_cols: Union[List[str], None] = None,
    attribute_col: str = ATTRIBUTE_ID,
    value_col: str = VALUE,
) -> DataFrame:
    """
    Convert dataframe from wide format to long format (equivalent of pd.melt).

    Params:
    ----------
        data: Dataframe
            Dataframe in wide format
        id_cols: Union[str, List[str]]
            Name of the columns to keep as id columns in the long format
        value_cols: Union[List[str], None], default = None
            Name of the columns to be converted to [attribute, value] columns in the long format.
            If None, convert all columns that are not in the id_cols list
        attribute_col: str
            Name of the attribute column in the long format
        value_col: str
            name of the value column in the long format

    Returns:
    ----------
        DataFrame: Long-formatted dataframe with schema [id_columns, attribute_col, value_col]
    """
    if isinstance(id_cols, str):
        id_cols = [id_cols]
    if value_cols is None:
        value_cols = [column for column in data.columns if column not in id_cols]

    attribute_values = F.create_map(
        *list(
            chain.from_iterable(
                [[F.lit(column), F.col(column)] for column in value_cols]
            )
        )
    )
    data = (
        data.select(*id_cols, F.explode(attribute_values))
        .withColumnRenamed("key", attribute_col)
        .withColumnRenamed("value", value_col)
    )
    return data


def column_to_list(data: DataFrame, column: str, drop_duplicates: bool = True) -> List:
    """
    Convert distinct values of a column to list.

    Params:
        data(DataFrame): Contains the column values to be extracted
        column(str): Column name
        drop_duplicates(bool): Drop duplicate values, default = True

    Return:
        values: List Values in the column
    """
    if drop_duplicates:
        return [data[0] for data in data.select(column).distinct().collect()]
    else:
        return [data[0] for data in data.select(column).collect()]
