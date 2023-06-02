#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import datetime

from enum import Enum
from functools import reduce
from typing import Dict

import pandas as pd
import pyspark.sql.functions as F

from faker import Faker
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

import transparency_engine.modules.data_shaper.spark_transform as transform
from transparency_engine.pipeline.schemas import ATTRIBUTE_ID, TIME_PERIOD, VALUE


"""
This module contains functions to generate fake data for some common data types 
(e.g. company name, email, address, phone number)
"""


class AttributeTypes(str, Enum):
    """
    Enum for entity attribute types.
    """

    COMPANY_NAME = "company_name"
    ADDRESS = "address"
    PHONE = "phone"
    EMAIL = "email"
    BENEFICIAL_OWNER = "beneficial_owner"
    DIRECTOR = "director"

    @staticmethod
    def from_string(attribute_type: str):
        """
        Converts a string to an entity attribute type.

        Params:
        ----------
        attribute_type : str
            The string to convert.

        Returns
        -------
        EntityMeasures
            The EntityMeasures enum.
        """
        if attribute_type == "company":
            return AttributeTypes.COMPANY_NAME
        elif attribute_type == "address":
            return AttributeTypes.ADDRESS
        elif attribute_type == "phone":
            return AttributeTypes.PHONE
        elif attribute_type == "email":
            return AttributeTypes.EMAIL
        elif attribute_type == "beneficial_owner":
            return AttributeTypes.BENEFICIAL_OWNER
        elif attribute_type == "director":
            return AttributeTypes.DIRECTOR
        else:
            raise ValueError(f"Unsupported attribute attribute type: {attribute_type}")


def __fake_value(
    fake: Faker,
    original_value: str,
    attribute_type: AttributeTypes,
    separator: str = ":",
    locale: str = "en_US",
) -> str:
    """
    Replace original attribute value with a fake value based on the attribute type.

    Params:
        original_value (str): Original attribute value
        attribute_type (AttributeType): type of attribute
        separator (str): join token to generate a new value, default = ':'

    Returns:
        str: Fake value
    """
    if attribute_type == AttributeTypes.COMPANY_NAME:
        return fake.unique.company().upper()
    elif attribute_type == AttributeTypes.ADDRESS:
        return _fake_address(fake, locale).replace("\r\n", "").upper()
    elif attribute_type == AttributeTypes.EMAIL:
        return fake.unique.safe_email().upper()
    elif attribute_type == AttributeTypes.PHONE:
        return fake.unique.phone_number().upper()
    elif attribute_type in [AttributeTypes.BENEFICIAL_OWNER, AttributeTypes.DIRECTOR]:
        return f"{fake.unique.name().upper()}(ID:{original_value})"
    else:
        return f"{attribute_type}{separator}{original_value}"


def _fake_address(fake: Faker, locale: str):
    building_number = fake.building_number()
    street = fake.street_name().upper()
    city = fake.city().upper()
    postcode = fake.postcode().upper()
    current_country = fake.current_country()
    if locale == "en_US":
        state = fake.state().upper()
        return " ".join(
            [building_number, street, city, state, postcode, current_country]
        )
    else:
        return " ".join([building_number, street, city, postcode, current_country])


def map_attributes(
    fake: Faker,
    data: DataFrame,
    attribute_col: str = ATTRIBUTE_ID,
    value_col: str = VALUE,
    separator: str = ":",
    locale: str = "en_US",
) -> DataFrame:
    """
    Map attribute values to fake data.

    Params:
        fake: Faker
        data (DataFrame): DataFrame with columns [EntityID, AttributeID, Value]
        attribute_col (str): name of the attribute ID column
        value_col (str): name of the value column
        separator (str): join token to create new value in teh form of {attribute_type}{separator}{attribute_value}
        locale (str): Faker locale

    Returns:
        result_df (DataFrame): updated dataframe
    """
    attribute_list = transform.column_to_list(data=data, column=attribute_col)

    new_data = []
    for attribute in attribute_list:
        attribute_data = data.filter(F.col(attribute_col) == attribute)
        attribute_values = transform.column_to_list(data=attribute_data, column=VALUE)

        # map original value to fake value
        value_mapping: Dict[str, str] = {}
        for value in attribute_values:
            new_value = __fake_value(fake, value, attribute, separator, locale)
            value_mapping[value] = new_value

        map_value_udf = F.udf(lambda value: value_mapping[value], StringType())
        attribute_data = attribute_data.withColumn(VALUE, map_value_udf(VALUE))
        new_data.append(attribute_data)
    result_df = reduce(DataFrame.unionAll, new_data)
    return result_df


def map_time(data: DataFrame, time_col: str = TIME_PERIOD) -> DataFrame:
    """
    Map time index to actual time with format yyyy-mm

    Params:
        data (DataFrame): DataFrame with a time column
        time_col (str): name of the time column

    Returns:
        result_df (DataFrame): updated dataframe
    """
    time_periods = transform.column_to_list(data=data, column=time_col)
    time_periods = [int(period) for period in time_periods]
    time_periods.sort()

    # convert period index to time period
    last_time = datetime.datetime.now() - datetime.timedelta(days=31)
    period_range = (
        pd.date_range(
            end=last_time.strftime("%Y-%m-%d"), periods=len(time_periods), freq="MS"
        )
        .strftime("%Y-%m")
        .tolist()
    )

    map_time_udf = F.udf(lambda period: period_range[int(period)], StringType())
    result_df = data.withColumn(TIME_PERIOD, map_time_udf(TIME_PERIOD))
    return result_df
