#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from enum import Enum
from math import pow
from typing import List, Union


class ArithmeticOperators(str, Enum):
    """
    Enum for types of arithmetic operators.
    """

    SUM = "sum"
    MULTIPLICATION = "multiplication"

    @staticmethod
    def from_string(operator_type: str):
        """
        Converts a string to an arithmetic operator enum.

        Params
        ----------
        operator_type : str
            The string to convert.

        Returns
        -------
        ArithmeticOperators
            The ArithmeticOperators enum.
        """
        if operator_type == "sum":
            return ArithmeticOperators.SUM
        elif operator_type == "multiplication":
            return ArithmeticOperators.MULTIPLICATION
        else:
            raise ValueError(f"Unsupported operator type: {operator_type}")


def weighted_sum(
    variables: List[float], weights: Union[List[float], None] = None
) -> float:
    """
    Compute the weighted sum = sum(var(i) * weight(i)).

    Params:
    -----------
        variables: List[float]
            List of variables
        weights:  Union[List[float], None], default = None
            weight of each variable. If None, all variables have weight = 1

    Returns:
    -----------
        float weighted sum
    """
    if weights and len(weights) != len(variables):
        raise ValueError("Mismatches between variable list and weight list")
    if not weights:
        weights = [1] * len(variables)

    result: float = 0.0
    for var, weight in zip(variables, weights):
        result += weight * var
    return result


def weighted_multiplication(
    variables: List[float], weights: Union[List[float], None] = None
) -> float:
    """
    Compute the weighted multiplication = mul(pow(var, weight)).

    Params:
    ----------
        variables: List[float]
            List of variables
        weights:  Union[List[float], None], default = None
            weight of each variable. If None, all variables have weight = 1

    Returns:
    -----------
        float weighted sum
    """
    if weights and len(weights) != len(variables):
        raise ValueError("Mismatches between variable list and weight list")
    if not weights:
        weights = [1] * len(variables)

    result: float = 1.0
    for var, weight in zip(variables, weights):
        result *= pow(var, weight)
    return result
