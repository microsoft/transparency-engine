#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging
import random

from collections import defaultdict
from typing import Dict, List, Set, Tuple

import networkx as nx
import numpy as np
import scipy.stats as stats

from dataclasses import dataclass
from faker import Faker
from numpy.random import poisson
from pyspark.sql import DataFrame

from transparency_engine.modules.data_generator.data_replacement import (
    map_attributes,
    map_time,
)
from transparency_engine.pipeline.schemas import (
    DYNAMIC_SCHEMA,
    ENTITY_SCHEMA,
    ENTITY_WEIGHT,
    METADATA_SCHEMA,
    REVIEW_FLAG_METADATA_SCHEMA,
    REVIEW_FLAG_SCHEMA,
    STATIC_SCHEMA,
)
from transparency_engine.spark.utils import spark


logger = logging.getLogger(__name__)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)


@dataclass
class StaticAttributeConfig:
    """
    Configuration to genenerate static attributes (e.g. name, phone, email) for an entity.

    Params:
    -----------
        id: str
            Attribute id
        name: str
            Attribute name
        description: str
            Attribute description
        expected_values: int, default = 1
            Expected number of attribute values per entity under a Poisson distribution
        share_prob: float, default = 0.5
            Probabilty of sharing each attribute value with a related entity
    """

    id: str
    name: str
    description: str
    expected_values: int = 1
    share_prob: float = 0.5


@dataclass
class DynamicAttributeConfig:
    """
    Configuration to genenerate dynamic attributes (e.g. bidding activitities, transactions) for an entity.

    Params:
    -----------
        id: str
            Attribute id
        name: str
            Attribute name
        description: str
            Attribute description
        expected_values: int, default = 1
            Expected number of action values per entity per time period under a Poisson distribution
        new_action_weight: float, default = 0.5
            Weight of a net new action.
        related_current_action_weight: float, default = 0.5
            Weight of the current action of the related entities
        own_past_action_weight: float: default = 0.5
            Weight of the entity's past action
        related_past_action_weight: float, default = 0.5
            Weight of the past action of the related entities
        community_past_action_weight: float, default = 0.5
            Weight of the past action of the entity's community.
    """

    id: str
    name: str
    description: str
    expected_values: int = 1
    new_action_weight: float = 0.5
    related_current_action_weight: float = 0.5
    own_past_action_weight: float = 0.5
    related_past_action_weight: float = 0.5
    community_past_action_weight: float = 0.5

    def compute_thresholds(self) -> List[float]:
        """
        Calculate thresholds used for sampling different action types.

        Returns:
            List[float]: Threshold list
        """
        weights = [
            self.new_action_weight,
            self.related_current_action_weight,
            self.own_past_action_weight,
            self.related_past_action_weight,
            self.community_past_action_weight,
        ]
        sum_of_weights = sum(weights)
        cumulative = 0.0
        thresholds: List[float] = []
        for weight in weights:
            normalized_weight = weight / sum_of_weights
            thresholds.append(cumulative + normalized_weight)
            cumulative += normalized_weight
        return thresholds


@dataclass
class DataSourceConfig:
    """
    Configuration for data sources (to be used to generate data description).

    Params:
        name: str
            Data source name
        description: str
            Data source description
    """

    name: str
    description: str


@dataclass
class FlagConfig:
    """
    Configuration for an entity's review flags (e.g., red flags).

    Params:
        id (str): Flag id
        name (str): Flag name
        category (str): Flag category
        weight (str): Flag weight, default = 1.0
        description (str): Flag description
        is_supporting_flag (bool): Whether this is a supporting flag, default = False
        data_source (DataSourceConfig): Source of the flag data
        initial_assignment_prob (float): Initial probability that an entity will have this flag, default = 0.05
    """

    id: str
    category: str
    name: str
    description: str
    data_source: DataSourceConfig
    is_supporting_flag: bool = False
    weight: float = 1.0
    initial_assignment_prob: float = 0.05


@dataclass
class EntityConfig:
    """
    Configuration for the entity weight.

    Params:
        name (str): Name of the entity weight
        description (str): Description of the entity weight
        min_weight (float): Min value of the entity weight
        max_weight (float): Max value of the entity weight
        mean_weight: Mean value of the entity weight
        standard_deviation_weight: Standard deviation of the entity weight
    """

    name: str
    description: str
    min_weight: float = 0.01
    max_weight: float = 1.0
    mean_weight: float = 0.5
    standard_deviation_weight: float = 0.1


@dataclass
class DataGeneratorConfig:
    """
    Data configurations for the synehtic data generator.

    Params:
        entity_label: str
            Type of entity (e.g. company)
        entity_weight: EntityConfig
            Configuration for entity weights, to be used for entity network scoring
        static_attributes: List[StaticAttributeConfig]
            List of static attributes
        dynamic_attributes: List[DynamicAttributeConfig]
            List of dynamic attributes
        flags: List[FlagConfig]
            List of entity response flags
        assigned_flag_boost: float, default = 0.1
            If flags are assigned to an entity, sample all flags again boosted by this probability
        observed_periods: int, default = 10
            Number of time periods that entity actions are observed
        number_of_entities: int, default = 100
            How many entities to create
        number_of_communities: int, default = 7
            How many communities the entities should form
        baseline_community_weight: float, default = 0.2
            Weight used to adjust graph community size. Larger values increase the minimum community size and make community sizes more uniform
        min_intra_community_connection_prob: float, default = 0.3
            Min probability of intra-community connection
        max_intra_community_connection_prob: float, default = 0.7
            Mac probability of intra-community connection
        min_inter_community_connection_prob: float, default = 0.0
            Min probability of inter-community connection
        max_inter_community_connection_prob: float, default = 0.01
            Mac probability of inter-community connection
        related_activity_connection_prob: float, default = 0.4
            How likely a ground truth relationship will show similar activity
        similar_activity_partition_prob: float, default = 0.3
            How likely companies with similar activity act in different time periods
        related_attribute_connection_prob: float = 0.7
            How likely a ground truth relationship will show shared attributes
        separator: str, default = ":"
            Token used to create output values in the form of {attribute_type}{separator}{attribute_value}
    """

    entity_label: str
    entity_weight: EntityConfig
    static_attributes: List[StaticAttributeConfig]
    dynamic_attributes: List[DynamicAttributeConfig]
    flags: List[FlagConfig]
    assigned_flag_boost: float = 0.1
    observed_periods: int = 10
    number_of_entities: int = 500
    number_of_communities: int = 15

    # relationship_forming_parameters
    baseline_community_weight: float = 0.2
    min_intra_community_connection_prob: float = 0.3
    max_intra_community_connection_prob: float = 0.7
    min_inter_community_connection_prob: float = 0.0
    max_inter_community_connection_prob: float = 0.01
    related_activity_connection_prob: float = 0.4
    similar_activity_partition_prob: float = 0.3
    related_attribute_connection_prob: float = 0.7
    separator: str = ":"
    locale: str = "en_US"


@dataclass
class TransparencyEngineData:
    entity: DataFrame
    static_attributes: DataFrame
    dynamic_attributes: DataFrame
    flags: DataFrame
    attribute_metadata: DataFrame
    flag_metadata: DataFrame


class SyntheticDataGenerator:
    """
    User inputs fall into 3 categories:

    (1) Define the data domain
    - The types of attributes that related entities may share (e.g., name, email).
    - The types of actions for which related entities may show similar patterns over time (e.g., transactions, interactions, communications).
    - The observation period (together with the count of observed periods) for which the actions of entities are assessed for their similarity.
    - The review flags (either "red flag" risks or "green flag" opportunities, but not both) that may be observed for each entity.

    (2) Synthesize the graph of ground truth relationships between entities
    - The target number of entities and the number of communities define the overall structure of entity relationships.
    - Relationship-forming parameters define the probabilities of connections for entities within and between communities.

    (3) Instantiate ground-truth relationships with observed relationships
    - Attribute-observing parameters define the expected numbers of static attributes of each type observed for each entity.
    - Action-observing parameters define the expected numbers of dynamic attributes of each type observed for each entity in each period.
    - Relationship-reflecting parameters define the probabilities of entity relationships being reflected by the sharing of attributes.

    The outputs of data synthesis are represented in 5 tables:

    (1) An Entity table with schema [EntityID, EntityWeight]
    (1) A `static attribute table` with schema [EntityID, AttributeID, Value]
    (2) A `dynamic attribute table` with schema [EntityID, AttributeID, Value, TimePeriod]
    (3) A `flags table` with schema [EntityID, FlagID, Evidence]
    (4) A flag metadata table with schema [FlagID, Category, Name, Description, IsSupportingFlag, FlagWeight]
    (5) An attribute metadata table with schema [AttributeID, Name, Description]
    """

    def __init__(self, configs: DataGeneratorConfig):
        self.configs = configs

    def generate_data(self) -> TransparencyEngineData:
        """
        Generate synthetic data for 5 tables needed for the Transparence Engine.

        Returns:
            synthetic_data: TransparencyEngineData The generated dataframes
        """
        fake = Faker(self.configs.locale)

        entity_df = self.__synthesize_entities()
        logger.info(f"Entity data generated: {entity_df.count()}")

        entity_to_community, community_sizes = self.__create_entity_communities()
        static_links, dynamic_links = self.__create_ground_truth_graph(community_sizes)

        static_attribute_df = self.__synthesize_static_attributes(
            static_links
        ).dropDuplicates()
        static_attribute_df = map_attributes(
            fake=fake,
            data=static_attribute_df,
            separator=self.configs.separator,
            locale=self.configs.locale,
        )
        logger.info(f"Static attribute data generated: {static_attribute_df.count()}")

        dynamic_attribute_df = self.__synthesize_dynamic_attributes(
            entity_to_community, dynamic_links
        )
        dynamic_attribute_df = map_attributes(
            fake=fake,
            data=dynamic_attribute_df,
            separator=self.configs.separator,
            locale=self.configs.locale,
        ).dropDuplicates()
        dynamic_attribute_df = map_time(dynamic_attribute_df)
        logger.info(f"Dynamic attribute data generated: {dynamic_attribute_df.count()}")

        flag_df = self.__synthesize_flags().dropDuplicates()
        logger.info(f"Entity flag data generated: {flag_df.count()}")

        flag_metadata_df = self.__synthesize_flag_metadata()
        logger.info(f"Flag metadata generated: {flag_metadata_df.count()}")

        attribute_metadata_df = self.__synthesize_attribute_metadata()
        logger.info(f"Attribute metadata generated: {attribute_metadata_df.count()}")

        synthetic_data = TransparencyEngineData(
            entity=entity_df,
            static_attributes=static_attribute_df,
            dynamic_attributes=dynamic_attribute_df,
            flags=flag_df,
            flag_metadata=flag_metadata_df,
            attribute_metadata=attribute_metadata_df,
        )
        return synthetic_data

    def __create_entity_communities(self):
        """
        Create an entity list and partition the entities into communities, with each community taking a random size.

        Params:
            configs: DataGeneratorConfig
                Configuration for the synthetic data generator.

        Returns:
            community_data, community_size: DataFrame
                DataFrame with schema [EntityID, CommunityID]
        """
        # Partition the entities into communities, with each community taking a random size
        community_weights = [
            self.configs.baseline_community_weight + random.random()  # nosec - B311
            for i in range(self.configs.number_of_communities)
        ]
        total_community_weights = sum(community_weights)
        raw_community_sizes = sorted(
            [
                int(
                    np.round(
                        i * self.configs.number_of_entities / total_community_weights
                    )
                )
                for i in community_weights
            ],
            reverse=True,
        )

        # Adjust raw community_sizes to match specified number of entities
        entity_delta = self.configs.number_of_entities - sum(raw_community_sizes)
        community_sizes = list(raw_community_sizes)
        if entity_delta < 0:
            # too many entities, remove from largest community
            community_sizes[0] += entity_delta
        else:
            # add to the smallest community
            community_sizes[-1] += entity_delta

        # save community data
        entity_id = 0
        entity_to_community: Dict[int, int] = {}
        for community_id, size in enumerate(community_sizes):
            for i in range(size):
                entity_to_community[entity_id] = community_id
                entity_id += 1
        return entity_to_community, community_sizes

    def __create_ground_truth_graph(
        self, community_sizes: List[int]
    ) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
        """
        Generate a ground-truth graph using a stochastic block model.

        Returns:
            static_relationship_links, dynamic_relationship_links: Tuple[Dict[int, List[int]], Dict[int, List[int]]]
                Dictionaries containing related entities for each entity.
        """
        # generate density of edges going from the nodes of community i to nodes of community j
        # and create graph using a stochastic block model
        community_link_to_probability = {}
        for i in range(self.configs.number_of_communities):
            for j in range(i, self.configs.number_of_communities):
                if i == j:
                    p = random.uniform(  # nosec - B311
                        self.configs.min_intra_community_connection_prob,
                        self.configs.max_intra_community_connection_prob,
                    )
                else:
                    p = random.uniform(  # nosec - B311
                        self.configs.min_inter_community_connection_prob,
                        self.configs.max_inter_community_connection_prob,
                    )
                community_link_to_probability[(i, j)] = p
                community_link_to_probability[(j, i)] = p
        community_link_probabilities = [
            [
                community_link_to_probability[(i, j)]
                for j in range(self.configs.number_of_communities)
            ]
            for i in range(self.configs.number_of_communities)
        ]
        G = nx.stochastic_block_model(community_sizes, community_link_probabilities)

        # randomize which edges represent static relationship links and which edges represent dynamic relationship links
        static_relationship_links = defaultdict(list)
        dynamic_relationship_links = defaultdict(list)
        for source, target in G.edges():
            if (
                random.random() # nosec - B311
                < self.configs.related_attribute_connection_prob  
            ):
                static_relationship_links[source].append(target)
                static_relationship_links[target].append(source)
            if (
                random.random() # nosec - B311
                < self.configs.related_activity_connection_prob  
            ):
                dynamic_relationship_links[source].append(target)
                dynamic_relationship_links[target].append(source)
        return (static_relationship_links, dynamic_relationship_links)

    def __synthesize_flags(self) -> DataFrame:
        """
        Generate entity's review flags, including mechanism to boost
        the chance of having multiple flags assigned for an entity.
        """
        flag_records = []
        for entity_id in range(self.configs.number_of_entities):
            assigned_flags = set()
            for flag in self.configs.flags:
                if random.random() < flag.initial_assignment_prob:  # nosec - B311
                    description = f"{flag.data_source.name}: Entity {entity_id} is linked to flag {flag.name} in {flag.data_source.description}"
                    assigned_flags.add((flag.id, description))

            # if entity has been assigned a flag, boost the probability of having more flags
            if len(assigned_flags) > 0:
                for flag in self.configs.flags:
                    if (
                        random.random()  # nosec - B311
                        < flag.initial_assignment_prob
                        + self.configs.assigned_flag_boost
                    ):
                        description = f"{flag.data_source.name}: Entity {entity_id} is linked to flag {flag.name} in {flag.data_source.description}"
                        assigned_flags.add((flag.id, description))
            for flag_id, description in assigned_flags:
                flag_records.append([str(entity_id), str(flag_id), str(description)])
        flag_df = spark.createDataFrame(flag_records, schema=REVIEW_FLAG_SCHEMA)
        return flag_df

    def __synthesize_static_attributes(
        self, static_relationship_links: Dict[int, List[int]]
    ) -> DataFrame:
        """
        Instantiate ground-truth static relationship with observed static relationships
        """
        attribute_records = []

        # traking attribute values across entities
        attribute_next_values = {
            attribute.id: 0 for attribute in self.configs.static_attributes
        }

        for entity_id in range(self.configs.number_of_entities):
            for attribute in self.configs.static_attributes:
                # generate number of values this entity has for this attribute
                n_attribute_values = poisson(attribute.expected_values)
                for _ in range(n_attribute_values):
                    new_value = attribute_next_values[attribute.id]
                    attribute_next_values[attribute.id] += 1
                    attribute_records.append(
                        [
                            str(entity_id),
                            str(attribute.id),
                            str(new_value),
                        ]
                    )

                    # check if this new attribute value should be shared with other related entities
                    to_share = random.random() < attribute.share_prob  # nosec - B311
                    if to_share and len(static_relationship_links[entity_id]) > 0:
                        selected_related = random.choice(  # nosec - B311
                            static_relationship_links[entity_id]
                        )
                        attribute_records.append(
                            [
                                str(selected_related),
                                str(attribute.id),
                                str(new_value),
                            ]
                        )

        attribute_df = spark.createDataFrame(
            data=attribute_records, schema=STATIC_SCHEMA
        )
        return attribute_df

    def __synthesize_dynamic_attributes(
        self,
        entity_to_community: Dict[int, int],
        dynamic_relationship_links: Dict[int, List[int]],
    ) -> DataFrame:
        """
        Instantiate ground-truth dynamic relationships with observed relationships.
        """
        attribute_list = [attribute.id for attribute in self.configs.dynamic_attributes]
        period_list = range(self.configs.observed_periods)
        entity_list = range(self.configs.number_of_entities)
        attribute_next_values = {attribute: 0 for attribute in attribute_list}
        all_values: Dict = {
            period: {
                entity: {attribute: [] for attribute in attribute_list}
                for entity in entity_list
            }
            for period in period_list
        }
        entity_values: Dict = {
            entity: {attribute: [] for attribute in attribute_list}
            for entity in entity_list
        }

        # generate attributes for each entity in each period
        attribute_thresholds = self.__compute_dynamic_action_thresholds()
        for period in range(self.configs.observed_periods):
            for entity_id in range(self.configs.number_of_entities):
                related_entities = dynamic_relationship_links[entity_id]
                community_entities = get_community_entities(
                    entity_to_community, entity_id
                )

                for attribute in self.configs.dynamic_attributes:
                    thresholds = attribute_thresholds[attribute.id]
                    n_attribute_values = poisson(attribute.expected_values)
                    for _ in range(n_attribute_values):
                        sampled_index = sample_with_thresholds(thresholds)
                        if sampled_index == 0:
                            # add net new action
                            new_value = attribute_next_values[attribute.id]
                            attribute_next_values[attribute.id] += 1
                            sampled = new_value
                        else:
                            candidates = self.__sample_dynamic_attributes(
                                all_values=all_values,
                                entity_values=entity_values,
                                entity_id=entity_id,
                                sampled_index=sampled_index,
                                period=period,
                                attribute=attribute,
                                related_entities=related_entities,
                                community_entities=community_entities,
                            )
                            if len(candidates) > 0:
                                # we found values to sample
                                sampled = random.choice(  # nosec - B311
                                    list(candidates)
                                )
                            else:
                                # need to create a new value and share it
                                new_value = attribute_next_values[attribute.id]
                                attribute_next_values[attribute.id] += 1
                                sampled = new_value
                                if len(related_entities) > 0:
                                    # share with related if possible
                                    selected_related = random.choice(  # nosec - B311
                                        related_entities
                                    )
                                    all_values[period][selected_related][
                                        attribute.id
                                    ].append(sampled)
                                elif len(community_entities) > 0:
                                    # otherwise share with community
                                    selected_related = random.choice(  # nosec - B311
                                        community_entities
                                    )
                                    all_values[period][selected_related][
                                        attribute.id
                                    ].append(sampled)
                                    entity_values[selected_related][
                                        attribute.id
                                    ].append(sampled)

                        # Add the sampled action for the current entity
                        all_values[period][entity_id][attribute.id].append(sampled)
                        entity_values[entity_id][attribute.id].append(sampled)

        # partition relationships
        all_values = self.__partition_dynamic_attributes(
            all_values, dynamic_relationship_links, attribute_list
        )
        return self.__write_dynamic_attributes(all_values)

    def __partition_dynamic_attributes(
        self,
        period_values: Dict,
        dynamic_relationship_links: Dict,
        attribute_list: List[str],
    ) -> Dict:
        """
        Simulate overlapping activity patterns between entity pairs that have a dynamic relationship link.

        Params:
            period_values: Dict
                Contains all attribute values of all entities for each period
            entity_attributes: Dict
                Contains all attribute values of each entity
            dyanmic_relationship_links: Dict
                Contains all relationship links for each entity

        Returns:
            Tuple[Dict, Dict]: Updated period attributes
        """
        partitioned_entities = set()
        updated_period_values = {}
        all_links = set(
            [
                (entity, related) if entity < related else (related, entity)
                for entity, related_entities in dynamic_relationship_links.items()
                for related in related_entities
            ]
        )
        for (entity, related) in all_links:
            if entity in partitioned_entities or related in partitioned_entities:
                continue
            if (
                random.random() # nosec - B311
                < self.configs.similar_activity_partition_prob  
            ):
                partitioned_entities.add(entity)
                partitioned_entities.add(related)

                # Select a partition period from the middle fifth of all periods
                partition_period = random.randint(  # nosec - B311
                    int(self.configs.observed_periods * 2 / 5),
                    int(self.configs.observed_periods * 3 / 5) + 1,
                )

                # Select a small gap size between the activity of the two entities
                gap = poisson(1)
                pause_period = int(partition_period - gap / 2)
                resume_period = int(partition_period + gap / 2)
                if (
                    pause_period > 0
                    and resume_period < self.configs.observed_periods - 1
                ):
                    for period, entity_attributes in period_values.items():
                        if period > pause_period:
                            # remove activity of the target entity after the pause period
                            for attribute in attribute_list:
                                entity_attributes[entity][attribute] = []

                        if period < resume_period:
                            # remove activity of the related entity before the resume period
                            for attribute in attribute_list:
                                entity_attributes[related][attribute] = []
                        updated_period_values[period] = entity_attributes
        return updated_period_values

    def __sample_dynamic_attributes(
        self,
        all_values: Dict,
        entity_values: Dict,
        entity_id: int,
        sampled_index: int,
        period: int,
        attribute: DynamicAttributeConfig,
        related_entities: List[int],
        community_entities: List[int],
    ) -> Set[int]:
        """
        Sample dynamic attributes for a given entity.

        Params:
            all_values: Dict
                Contains all attribute values of all entities for each period
            entity_attributes: Dict
                Contains all attribute values of each entity
            entity_id: int
                Target entity id
            sampled_index: int
                Determine the action type to sample
            period: int
                Current period
            attribute: DynamicAttributeConfig
                Dynamic attribute
            related_entities: List[int]
                Entities related to the current entity
            community_entities: List[int]
                Entities in the same community as the current entity

        Returns:
            Set[int]: Candidate values for the dynamic attribute
        """
        candidates = set()
        if sampled_index == 1:
            # sample from current values of the related entities
            candidates = (
                set()
                .union(
                    *[
                        all_values[period][related][attribute.id]
                        for related in related_entities
                    ]
                )
                .difference(all_values[period][entity_id][attribute.id])
            )
        elif sampled_index == 2:
            # sample from the entity's own past attribute values
            candidates = set(entity_values[entity_id][attribute.id]).difference(
                all_values[period][entity_id][attribute.id]
            )
        elif sampled_index == 3:
            # sample from past attribute values of the related entities
            candidates = (
                set()
                .union(
                    *[
                        entity_values[related][attribute.id]
                        for related in related_entities
                    ]
                )
                .difference(
                    set().union(
                        *[
                            all_values[period][related][attribute.id]
                            for related in related_entities
                        ]
                    )
                )
                .difference(all_values[period][entity_id][attribute.id])
            )
        else:
            # sample from past attribute values of entities in the same community
            candidates = (
                set()
                .union(
                    *[
                        entity_values[related][attribute.id]
                        for related in community_entities
                    ]
                )
                .difference(
                    set().union(
                        *[
                            all_values[period][related][attribute.id]
                            for related in community_entities
                        ]
                    )
                )
                .difference(all_values[period][entity_id][attribute.id])
            )

        return candidates

    def __write_dynamic_attributes(self, period_values: Dict) -> DataFrame:
        """
        Save the dynamic attributes to a Spark dataframe

        Params:
            period_values: Dict

        Returns:
            attribute_df: DataFrame
                DataFrame with schema [EntityID, AttributeID, Value, Period]
        """
        attribute_records = []
        for period, entity_attributes in period_values.items():
            for entity, attribute_values in entity_attributes.items():
                for attribute, values in attribute_values.items():
                    for value in values:
                        attribute_records.append(
                            [str(entity), str(attribute), str(value), str(period)]
                        )
        attribute_df = spark.createDataFrame(attribute_records, schema=DYNAMIC_SCHEMA)
        return attribute_df

    def __compute_dynamic_action_thresholds(self) -> Dict[str, List[float]]:
        """
        Compute thresholds used to sample values for each dynamic attribute.

        Returns:
            Dict[int, List[float]]: threshold values for each attribute.
        """
        thresholds = {}
        for attribute in self.configs.dynamic_attributes:
            thresholds[attribute.id] = attribute.compute_thresholds()
        return thresholds

    def __synthesize_flag_metadata(self) -> DataFrame:
        """
        Write flag metadata to a Spark dataframe.

        Returns:
            DataFrame: contains flag metadata
        """
        metadata_records = []
        for flag in self.configs.flags:
            metadata_records.append(
                [
                    str(flag.id),
                    str(flag.category),
                    str(flag.name),
                    str(flag.description),
                    bool(flag.is_supporting_flag),
                    float(flag.weight),
                ]
            )

        metadata_df = spark.createDataFrame(
            metadata_records, schema=REVIEW_FLAG_METADATA_SCHEMA
        )
        return metadata_df

    def __synthesize_attribute_metadata(self) -> DataFrame:
        """
        Write attribute metadata to a Spark dataframe.

        Returns:
            DataFrame: contains attribute metadata
        """
        metadata_records = []
        for static_attribute in self.configs.static_attributes:
            metadata_records.append(
                [
                    str(static_attribute.id),
                    str(static_attribute.name),
                    str(static_attribute.description),
                ]
            )

        for dynamic_attribute in self.configs.dynamic_attributes:
            metadata_records.append(
                [
                    str(dynamic_attribute.id),
                    str(dynamic_attribute.name),
                    str(dynamic_attribute.description),
                ]
            )

        metadata_records.append(
            [
                str(ENTITY_WEIGHT),
                str(self.configs.entity_weight.name),
                str(self.configs.entity_weight.description),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_records, schema=METADATA_SCHEMA)
        return metadata_df

    def __synthesize_entities(self) -> DataFrame:
        """
        Generate entity weights using a truncated normal distribution.

        Returns:
            entity_df: DataFrame
                Entity data with schema [EntityID, EntityWeight]
        """
        # generate weight for each entity
        lower = self.configs.entity_weight.min_weight
        upper = self.configs.entity_weight.max_weight
        mu = self.configs.entity_weight.mean_weight
        sigma = self.configs.entity_weight.standard_deviation_weight
        n_entities = self.configs.number_of_entities
        entity_weights = stats.truncnorm.rvs(
            (lower - mu) / sigma,
            (upper - mu) / sigma,
            loc=mu,
            scale=sigma,
            size=n_entities,
        )

        # create the Entity dataframe
        entity_records = []
        for entity_id in range(n_entities):
            entity_records.append([str(entity_id), float(entity_weights[entity_id])])
        entity_df = spark.createDataFrame(entity_records, schema=ENTITY_SCHEMA)
        return entity_df


def get_community_entities(
    entity_to_community: Dict[int, int], entity_id: int
) -> List[int]:
    """
    Helper function that returns a list of entities in the same community of the target entity id.

    Params:
        communit_df: DataFrame
            Contains [EntityID, CommunityID] data
        entity_id: int
            Index of the target entity

    Returns:
        List[int]: List of entity ids in the same community
    """
    target_community = entity_to_community[entity_id]
    community_entities = [
        entity
        for entity, community in entity_to_community.items()
        if community == target_community and entity != entity_id
    ]
    return community_entities


def sample_with_thresholds(thresholds: List[float]) -> int:
    """
    Helper function that sample an index given a list of threshold values.

    Params:
        thersholds: List[float]
            List of threshold values

    Returns:
        int: Sampled index
    """
    sampled_index = 0
    for index in range(len(thresholds)):
        if random.random() > thresholds[index]:  # nosec - B311
            continue
        else:
            sampled_index = index
        break
    return sampled_index

def write_synthetic_data(data_tables: Dict[str, DataFrame], output_path: str):
    """
    Write all data tables to CSV files

    Params:
        data_tables: Dict[str, DataFrame]
            Dictionary of table name and corresponding dataframe
        output_path: str
            Path to write the CSV files
    """
    for table in data_tables:     
        __write_to_csv(data_tables[table], output_path, table)

def __write_to_csv(data: DataFrame, output_path: str, filename: str) -> None:
    """
    Write dataframe to CSV with header.

    Params:
        data: Dataframe to write
        output_path: Path of the folder to save the csv file to
        filename: name of the csv file
    """
    temp_path = f"{output_path}/temp__{filename}"
    target_path = f"{output_path}/{filename}.csv"
    data.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    # get the csv part file
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path  # type: ignore
    fs = Path(temp_path).getFileSystem(sc._jsc.hadoopConfiguration())  # type: ignore
    csv_file = fs.globStatus(Path(temp_path + "/*.csv"))[0].getPath()
    fs.rename(csv_file, Path(target_path))
    fs.delete(Path(temp_path), True)