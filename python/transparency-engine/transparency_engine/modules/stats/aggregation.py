#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import statistics

from typing import Union

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window


@udf(returnType=FloatType())
def mean_list_udf(scores: list):
    """
    This function calculates the mean of a given list of numbers

    Parameters:
        scores (list): list of numbers whose mean is to be calculated

    Returns:
        float: mean of the list
    """
    return statistics.mean(scores)


def percent_rank(
    df: DataFrame,
    input_col: str,
    percent_col: Union[str, None] = None,
    min_percent: float = 0.1,
    reset_zero: bool = True,
    remove_null: bool = True,
) -> DataFrame:
    """
    This function calculates the percent rank of a given column in a DataFrame and returns the modified DataFrame.

    Parameters:
        df (DataFrame): The DataFrame on which the calculation is to be performed.
        input_col (str): The column name whose percent rank is to be calculated.
        percent_col (str): The column name to store the percent rank. Default is empty.
        min_percent (float): The minimum percent rank. Default is 0.1.
        reset_zero (bool): If true, sets percent rank to 100% for all values in the input_col that are 0 (assuming 0 is the minimum value). Default is True.
        remove_null (bool): If true, removes all the rows where the value of input_col is Null. Default is True.

    Returns:
        DataFrame: The modified DataFrame with percent rank added as a new column.
    """
    # calculate percent rank of a given column
    if remove_null:
        cleaned_df = df.filter(F.col(input_col).isNotNull())
    else:
        cleaned_df = df
    window = Window.partitionBy().orderBy(F.col(input_col).desc())
    if not percent_col:
        percent_col = f"{input_col}_pct"
    cleaned_df = cleaned_df.withColumn(
        percent_col, F.format_number((100 * F.percent_rank().over(window)), 1)
    )
    __round_up_percent_udf = F.udf(
        lambda percent_col: __round_up_percentile(percent_col, min_percent), FloatType()
    )
    cleaned_df = cleaned_df.withColumn(
        percent_col, __round_up_percent_udf(F.col(percent_col))
    )
    if reset_zero:
        # if reset zero is on, we set percent rank = 100% for all value col = 0 (assuming 0 is the minimum value)
        cleaned_df = cleaned_df.withColumn(
            percent_col,
            F.when(F.col(input_col) == 0, 100).otherwise(F.col(percent_col)),
        )
    cleaned_df = cleaned_df.select(input_col, percent_col).distinct()
    df = df.join(cleaned_df, on=input_col, how="left")
    return df


def normalize_max_scale(
    df: DataFrame, input_col: str, output_col: Union[str, None] = None
) -> DataFrame:
    """
    Normalize values in a column using a max scaler

    Parameters:
        df (DataFrame): The DataFrame on which the calculation is to be performed.
        input_col (str): The column name whose values are to be normalized.
        output_col (str): The column name to store the normalized values.

    Returns:
        DataFrame: The modified DataFrame with normalized values added as a new column.
    """
    if not output_col:
        output_col = f"{input_col}_max_scaled"
    stats = df.agg(F.max(input_col).alias("max"))
    df = df.join(F.broadcast(stats))
    df = df.withColumn(output_col, F.col(input_col) / F.col("max")).drop("max")
    return df


def normalize_rank(
    df: DataFrame,
    input_col: str,
    output_col: Union[str, None] = None,
    remove_null: bool = True,
    remove_zero: bool = False,
    reset_zero: bool = True,
) -> DataFrame:
    """
    This function calculates the normalized rank of a given column in a DataFrame and returns the modified DataFrame.

    Parameters:
        df (DataFrame): The DataFrame on which the calculation is to be performed.
        input_col (str): The column name whose normalized rank is to be calculated.
        output_col (str): The column name to store the normalized rank. Default is empty.
        remove_null (bool): If true, removes all the rows where the value of input_col is Null. Default is True.
        remove_zero (bool): If true, removes all the rows where the value of input_col is 0. Default is False.
        reset_zero (bool): If true, sets normalized rank to 0% for all values in the input_col that are 0 (assuming 0 is the minimum value). Default is True.

    Returns:
        DataFrame: The modified DataFrame with normalized rank added as a new column.
    """
    # calculate percent rank of a given column
    if remove_null:
        cleaned_df = df.filter(F.col(input_col).isNotNull())
    else:
        cleaned_df = df
    if remove_zero:
        cleaned_df = cleaned_df.filter(F.col(input_col) != 0)
    window = Window.partitionBy().orderBy(F.col(input_col))

    if not output_col:
        output_col = f"{input_col}_rank_scaled"
    cleaned_df = cleaned_df.withColumn(output_col, F.dense_rank().over(window))
    cleaned_df = normalize_max_scale(cleaned_df, output_col, output_col)
    if reset_zero:
        # if reset zero is on, we set percent rank = 0% for all value col = 0 (assuming 0 is the minimum value)
        cleaned_df = cleaned_df.withColumn(
            output_col, F.when(F.col(input_col) == 0, 0.0).otherwise(F.col(output_col))
        )
    cleaned_df = cleaned_df.select(input_col, output_col).distinct()
    df = df.join(cleaned_df, on=input_col, how="left")
    df = df.fillna(0, subset=output_col)
    return df


def __round_up_percentile(percent: float, min_threshold: float = 0.1) -> float:
    """
    A function to round up the given percentile value to a minimum threshold value.

    Parameters:
        percent (float): The percentile value to round up.
        min_threshold (float, optional): The minimum threshold value. Defaults to 0.1.

    Returns:
        float: The input percentile value rounded up to the minimum threshold, if necessary.
    """
    if percent is None:
        return -1.0
    return float(percent) if float(percent) >= min_threshold else min_threshold
