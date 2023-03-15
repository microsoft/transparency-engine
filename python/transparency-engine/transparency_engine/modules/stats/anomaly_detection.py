#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import pyspark.sql.functions as F

from pyspark.sql import DataFrame


"""
This module contains functions for detecting outliers in a DataFrame.
The functions in this module are designed to be used with PySpark DataFrames.
"""


def detect_anomaly_zscore(
    df: DataFrame, col: str, outlier_flag_col: str = "is_anomaly", min_zscore: float = 3
):
    """
    A PySpark function to detect outliers in a DataFrame using the Z-Score method.

    Parameters:
        df (DataFrame): The DataFrame containing the column to check for outliers.
        col (str): The name of the column to check for outliers.
        outlier_flag_col (str, optional): The name of the column to add to the DataFrame indicating whether a row is an outlier. Defaults to "is_anomaly".
        min_zscore (float, optional): The minimum z-score required for a value to be considered an outlier. Defaults to 3.

    Returns:
        DataFrame: The input DataFrame with an added column indicating whether each row is an outlier.
    """
    stats = df.agg(
        F.stddev(col).alias("std"), F.avg(col).alias("mean"), F.max(col).alias("max")
    )
    df = df.join(F.broadcast(stats))
    df = df.withColumn("z_score", (F.col(col) - F.col("mean")) / F.col("std")).drop(
        *["std", "mean", "max"]
    )
    df = df.withColumn(
        outlier_flag_col, F.when(F.col("z_score") >= min_zscore, 1).otherwise(0)
    ).drop("z_score")
    return df
