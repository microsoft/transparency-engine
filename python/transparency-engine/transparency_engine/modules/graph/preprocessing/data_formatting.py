#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def format_attribute_data(
    entity_attribute_data: DataFrame,
    value_col: str = "Value",
) -> DataFrame:
    """
    Format the entity attribute values in the input data.

    Params:
        entity_attribute_data: DataFrame
            Input dataframe in the format of [EntityID, AttributeID, Value]

    Returns:
        Dataframe: Cleaned up attribute data.
    """
    cleaned_data = entity_attribute_data.na.drop()
    cleaned_data = cleaned_data.withColumn(value_col, F.upper(F.trim(value_col)))
    return cleaned_data
