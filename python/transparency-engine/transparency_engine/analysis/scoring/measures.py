#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from enum import Enum
from typing import Dict, List, Union

from dataclasses import dataclass

from transparency_engine.modules.stats.arithmetic_operators import (
    ArithmeticOperators,
    weighted_multiplication,
    weighted_sum,
)


class EntityMeasures(str, Enum):
    """
    Enum for measures used for individual entity scoring.
    """

    # count of all review flags
    FLAG_COUNT = "flag_count"

    # sum of weights of all review flags
    FLAG_WEIGHT = "flag_weight"

    # entity weight
    ENTITY_WEIGHT = "entity_weight"

    @staticmethod
    def from_string(entity_measure_type: str):
        """
        Converts a string to an entity measures enum.

        Params
        ----------
        entity_measure_type : str
            The string to convert.

        Returns
        -------
        EntityMeasures
            The EntityMeasures enum.
        """
        if entity_measure_type == "flag_count":
            return EntityMeasures.FLAG_COUNT
        elif entity_measure_type == "flag_weight":
            return EntityMeasures.FLAG_WEIGHT
        elif entity_measure_type == "entity_weight":
            return EntityMeasures.ENTITY_WEIGHT
        else:
            raise ValueError(f"Unsupported entity measure type: {entity_measure_type}")

    @staticmethod
    def list():
        return list(map(lambda measure: measure.value, EntityMeasures))


class NetworkMeasures(str, Enum):
    """
    Enum for measures used for entity network scoring.
    """

    # count of all flags of the target entity
    TARGET_FLAG_COUNT = "target_flag_count"
    # sum of weights of all flags of the target entity
    TARGET_FLAG_WEIGHT = "target_flag_weight"
    # the weight of the target entity
    TARGET_ENTITY_WEIGHT = "target_entity_weight"
    # how many links connect the target entity to parts of the network containing review flags
    TARGET_FLAG_DEGREE = "target_flag_degree"
    # how many types of link that connect the target entity to parts of the network containing review flags
    TARGET_FLAG_DEGREE_TYPE = "target_flag_degree_type"

    # count of all flags of related entities
    RELATED_FLAG_COUNT = "related_flag_count"
    # count of all related entities that have a review flag
    RELATED_FLAGGED_COUNT = "related_flagged_count"
    # count of all related entities
    RELATED_ENTITY_COUNT = "related_entity_count"
    # sum of weights of all flags of related entities
    RELATED_FLAG_WEIGHT = "related_flag_weight"
    # sum of weights of all related entities that have a review flag
    RELATED_FLAGGED_WEIGHT = "related_flagged_weight"
    # sum of weights of all related entities
    RELATED_ENTITY_WEIGHT = "related_entity_weight"

    # count of all flags of directly-related entities
    DIRECT_FLAG_COUNT = "direct_flag_count"
    # count of all directly-related entities that have a review flag
    DIRECT_FLAGGED_COUNT = "direct_flagged_count"
    # count of all directly-related entities
    DIRECT_ENTITY_COUNT = "direct_entity_count"
    # sum of weights of all flags of directly-related entities
    DIRECT_FLAG_WEIGHT = "direct_flag_weight"
    # sum of weights of all directly-related entities that have a review flag
    DIRECT_FLAGGED_WEIGHT = "direct_flagged_weight"
    # sum of weights of all directly-related entities
    DIRECT_ENTITY_WEIGHT = "direct_entity_weight"

    # count of all flags of indirectly-related entities
    INDIRECT_FLAG_COUNT = "indirect_flag_count"
    # count of all indirectly-related entities that have a review flag
    INDIRECT_FLAGGED_COUNT = "indirect_flagged_count"
    # count of all indirectly-related entities
    INDIRECT_ENTITY_COUNT = "indirect_entity_count"
    # sum of weights of all indirectly-related flagged entities
    INDIRECT_FLAG_WEIGHT = "indirect_flag_weight"
    # sum of weights of all indirectly-related entities that have a review flag
    INDIRECT_FLAGGED_WEIGHT = "indirect_flagged_weight"
    # sum of weights of all indirectly-related entities
    INDIRECT_ENTITY_WEIGHT = "indirect_entity_weight"

    # count of all flags in the network
    NETWORK_FLAG_COUNT = "network_flag_count"
    # count of all entities that have a review flag in the network
    NETWORK_FLAGGED_COUNT = "network_flagged_count"
    # count of all entities in the network
    NETWORK_ENTITY_COUNT = "network_entity_count"
    # sum of weights of all flags in the network
    NETWORK_FLAG_WEIGHT = "network_flag_weight"
    # sum of weights of all entities that have a review flag in the network
    NETWORK_FLAGGED_WEIGHT = "network_flagged_weight"
    # sum of weights of all entities in the network
    NETWORK_ENTITY_WEIGHT = "network_entity_weight"

    # the computed score of the target entity
    TARGET_ENTITY_SCORE = "target_entity_score"
    # sum of computed scores for all related entities
    RELATED_ENTITY_SCORE = "related_entity_score"
    # sum of computed scores for all directly-related entities in the network
    DIRECT_ENTITY_SCORE = "direct_entity_score"
    # sum of computed scores for all indirectly-related entities in the network
    INDIRECT_ENTITY_SCORE = "indirect_entity_score"
    # sum of computed scores for all  entities in the network
    NETWORK_ENTITY_SCORE = "network_entity_score"

    @staticmethod
    def from_string(network_measure_type: str):
        """
        Converts a string to an entity measures enum.

        Params
        ----------
        network_measure_type : str
            The string to convert.

        Returns
        -------
        NetworkMeasures
            The NetworkMeasures enum.
        """
        measure_string_dict = {
            "target_flag_count": NetworkMeasures.TARGET_FLAG_COUNT,
            "target_flag_weight": NetworkMeasures.TARGET_FLAG_WEIGHT,
            "target_entity_weight": NetworkMeasures.TARGET_ENTITY_WEIGHT,
            "target_flag_degree": NetworkMeasures.TARGET_FLAG_DEGREE,
            "target_flag_degree_type": NetworkMeasures.TARGET_FLAG_DEGREE_TYPE,
            "related_flag_count": NetworkMeasures.RELATED_FLAG_COUNT,
            "related_flag_weight": NetworkMeasures.RELATED_FLAG_WEIGHT,
            "related_flagged_count": NetworkMeasures.RELATED_FLAGGED_COUNT,
            "related_flagged_weight": NetworkMeasures.RELATED_FLAGGED_WEIGHT,
            "related_entity_count": NetworkMeasures.RELATED_ENTITY_COUNT,
            "related_entity_weight": NetworkMeasures.RELATED_ENTITY_WEIGHT,
            "direct_flag_count": NetworkMeasures.DIRECT_FLAG_COUNT,
            "direct_flag_weight": NetworkMeasures.DIRECT_FLAG_WEIGHT,
            "direct_flagged_count": NetworkMeasures.DIRECT_FLAGGED_COUNT,
            "direct_flagged_weight": NetworkMeasures.DIRECT_FLAGGED_WEIGHT,
            "direct_entity_count": NetworkMeasures.DIRECT_ENTITY_COUNT,
            "direct_entity_weight": NetworkMeasures.DIRECT_ENTITY_WEIGHT,
            "indirect_flag_count": NetworkMeasures.INDIRECT_FLAG_COUNT,
            "indirect_flag_weight": NetworkMeasures.INDIRECT_FLAG_WEIGHT,
            "indirect_flagged_count": NetworkMeasures.INDIRECT_FLAGGED_COUNT,
            "indirect_flagged_weight": NetworkMeasures.INDIRECT_FLAGGED_WEIGHT,
            "indirect_entity_count": NetworkMeasures.INDIRECT_ENTITY_COUNT,
            "indirect_entity_weight": NetworkMeasures.INDIRECT_ENTITY_WEIGHT,
            "network_flag_count": NetworkMeasures.NETWORK_FLAG_COUNT,
            "network_flag_weight": NetworkMeasures.NETWORK_FLAG_WEIGHT,
            "network_flagged_count": NetworkMeasures.NETWORK_FLAGGED_COUNT,
            "network_flagged_weight": NetworkMeasures.NETWORK_FLAGGED_WEIGHT,
            "network_entity_count": NetworkMeasures.NETWORK_ENTITY_COUNT,
            "network_entity_weight": NetworkMeasures.NETWORK_ENTITY_WEIGHT,
            "target_entity_score": NetworkMeasures.TARGET_ENTITY_SCORE,
            "related_entity_score": NetworkMeasures.RELATED_ENTITY_SCORE,
            "direct_entity_score": NetworkMeasures.DIRECT_ENTITY_SCORE,
            "indirect_entity_score": NetworkMeasures.INDIRECT_ENTITY_SCORE,
            "network_entity_score": NetworkMeasures.NETWORK_ENTITY_SCORE,
        }

        if network_measure_type in measure_string_dict:
            return measure_string_dict[network_measure_type]
        else:
            raise ValueError(
                f"Unsupported network measure type: {network_measure_type}"
            )

    @staticmethod
    def list():
        return list(map(lambda measure: measure.value, NetworkMeasures))


class ScoringTypes(str, Enum):
    """
    Enum of types of final scores to be output from the analysis
    """

    FINAL_ENTITY_SCORE = "entity_score"
    FINAL_NETWORK_SCORE = "final_network_score"

    @staticmethod
    def from_string(score_type: str):
        """
        Converts a string to a scoring types enum.

        Params
        ----------
        score_type : str
            The string to convert.

        Returns
        -------
        ScoringTypes
            The ScoringTypes enum.
        """
        if score_type == "entity_score":
            return ScoringTypes.FINAL_ENTITY_SCORE
        elif score_type == "final_network_score":
            return ScoringTypes.FINAL_NETWORK_SCORE
        else:
            raise ValueError(f"Unsupported scoring type: {score_type}")


class NetworkMeasureCategories(str, Enum):
    """
    Enum of the categories of measure for network
    """

    DIRECT = "direct"
    INDIRECT = "indirect"

    @staticmethod
    def from_string(measure_category: str):
        """
        Converts a string to a measure category enum.

        Params
        ----------
        measure_category : str
            The string to convert.

        Returns
        -------
        NetworkMeasureCategories
            The NetworkMeasureCategories enum.
        """
        if measure_category == "direct":
            return NetworkMeasureCategories.DIRECT
        elif measure_category == "indirect":
            return NetworkMeasureCategories.INDIRECT
        else:
            raise ValueError(
                f"Unsupported network measure category type: {measure_category}"
            )


class NormalizationTypes(str, Enum):
    """
    Enum for types of normalization to be performed on a given measure.
    """

    UNNORMALIZED = "unnormalized"
    BY_MAX_VALUE = "max_scaled"
    BY_RANK_VALUE = "rank_scaled"

    @staticmethod
    def from_string(normalization_type: str):
        """
        Converts a string to a normalization enum.

        Params
        ----------
        normalization_type : str
            The string to convert.

        Returns
        -------
        NormalizationTypes
            The NormalizationTypes enum.
        """
        if normalization_type == "unnormalized":
            return NormalizationTypes.UNNORMALIZED
        elif normalization_type == "max_scaled":
            return NormalizationTypes.BY_MAX_VALUE
        elif normalization_type == "rank_scaled":
            return NormalizationTypes.BY_RANK_VALUE
        else:
            raise ValueError(f"Unsupported operator type: {normalization_type}")


@dataclass
class MeasureConfig:
    """Configuration for a single measure"""

    name: Union[EntityMeasures, NetworkMeasures]
    weight: float = 1.0
    normalization: NormalizationTypes = NormalizationTypes.BY_MAX_VALUE


@dataclass
class ScoringConfig:
    selected_measures: List[MeasureConfig]
    scoring_function: ArithmeticOperators = ArithmeticOperators.MULTIPLICATION


def compute_score(
    all_measure_values: Dict[str, float], configs: ScoringConfig
) -> float:
    """
    Compute entity score using an entity scoring function that aggregate different entity measures.

    Params:
    -------------
        all_measure_values: Dict[str, float]
            Dictionary of measure-value for all calculated entity measures
        configs: ScoringConfig

    Returns:
    --------------
    """
    selected_measure_values = []
    selected_measure_weights = []
    for measure_config in configs.selected_measures:
        if measure_config.name in all_measure_values:
            if measure_config.normalization != NormalizationTypes.UNNORMALIZED:
                selected_measure_values.append(
                    float(
                        all_measure_values[
                            f"{measure_config.name}_{measure_config.normalization}"
                        ]
                    )
                )
            else:
                selected_measure_values.append(
                    float(all_measure_values[measure_config.name])
                )
            selected_measure_weights.append(float(measure_config.weight))
        else:
            raise ValueError(f"Measure {measure_config.name} does not exist")

    score: float = 0.0
    if configs.scoring_function == ArithmeticOperators.SUM:
        score = weighted_sum(selected_measure_values, selected_measure_weights)
    elif configs.scoring_function == ArithmeticOperators.MULTIPLICATION:
        score = weighted_multiplication(
            selected_measure_values, selected_measure_weights
        )
    else:
        raise ValueError(
            f"Scoring function {configs.scoring_function} is not supported"
        )
    return score
