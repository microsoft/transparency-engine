#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from typing import Final, List, Dict

import pyspark.sql.functions as F

from pyspark.sql import DataFrame

from transparency_engine.modules.data_generator.synthetic_data_generator import (
    DataGeneratorConfig,
    DataSourceConfig,
    DynamicAttributeConfig,
    EntityConfig,
    FlagConfig,
    StaticAttributeConfig,
    SyntheticDataGenerator,
)
from transparency_engine.pipeline.schemas import ATTRIBUTE_ID


procurement_configs = DataGeneratorConfig(
    entity_label="company",
    static_attributes=[
        StaticAttributeConfig(
            id="company_name",
            name="name",
            description="company name",
            expected_values=2,
            share_prob=0.25,
        ),
        StaticAttributeConfig(
            id="phone",
            name="phone number",
            description="company phone number",
            expected_values=3,
            share_prob=0.5,
        ),
        StaticAttributeConfig(
            id="email",
            name="email",
            description="company email",
            expected_values=3,
            share_prob=0.5,
        ),
        StaticAttributeConfig(
            id="address",
            name="address",
            description="company address",
            expected_values=4,
            share_prob=0.25,
        ),
        StaticAttributeConfig(
            id="beneficial_owner",
            name="beneficial owner",
            description="beneficial owner of the company",
            expected_values=2,
            share_prob=0.75,
        ),
        StaticAttributeConfig(
            id="director",
            name="company director",
            description="director of the company",
            expected_values=2,
            share_prob=0.75,
        ),
    ],
    dynamic_attributes=[
        DynamicAttributeConfig(
            id="tender",
            name="tender",
            description="id of tender in public procurement",
            expected_values=5,
            new_action_weight=0.01,
            related_current_action_weight=10,
            own_past_action_weight=0,
            related_past_action_weight=0,
            community_past_action_weight=0,
        ),
        DynamicAttributeConfig(
            id="item",
            name="item",
            description="id of line item in public procurement",
            expected_values=5,
            new_action_weight=0.01,
            related_current_action_weight=20,
            own_past_action_weight=20,
            related_past_action_weight=10,
            community_past_action_weight=5,
        ),
        DynamicAttributeConfig(
            id="buyer",
            name="buyer",
            description="id of buyer in public procurement",
            expected_values=5,
            new_action_weight=0.01,
            related_current_action_weight=20,
            own_past_action_weight=20,
            related_past_action_weight=10,
            community_past_action_weight=5,
        ),
    ],
    entity_weight=EntityConfig(
        name="contract value",
        description="total contract value awarded to the entity in the past 5 years",
        min_weight=1e5,
        max_weight=1e8,
        mean_weight=5e6,
        standard_deviation_weight=1e6,
    ),
    flags=[
        FlagConfig(
            id="F1",
            category="judgements",
            name="fined",
            description="The company was fined",
            data_source=DataSourceConfig("OSINT", "open data sources"),
            is_supporting_flag=False,
            weight=1,
            initial_assignment_prob=0.05,
        ),
        FlagConfig(
            id="F2",
            category="judgements",
            name="sanctioned",
            description="The company was sanctioned",
            data_source=DataSourceConfig("OSINT", "open data sources"),
            is_supporting_flag=False,
            weight=5,
            initial_assignment_prob=0.03,
        ),
        FlagConfig(
            id="F3",
            category="judgements",
            name="litigated",
            description="The company was litigated",
            data_source=DataSourceConfig("OSINT", "open data sources"),
            is_supporting_flag=False,
            weight=1,
            initial_assignment_prob=0.03,
        ),
        FlagConfig(
            id="F4",
            category="ownership",
            name="front person",
            description="A company owner is a front person",
            data_source=DataSourceConfig("DNINT", "digital network intelligence data"),
            is_supporting_flag=False,
            weight=2,
            initial_assignment_prob=0.05,
        ),
        FlagConfig(
            id="F5",
            category="ownership",
            name="convicted criminal",
            description="A company owner is a convicted criminal",
            data_source=DataSourceConfig("OSINT", "open data sources"),
            is_supporting_flag=False,
            weight=5,
            initial_assignment_prob=0.03,
        ),
        FlagConfig(
            id="F6",
            category="ownership",
            name="politically-exposed person",
            description="A company owner is a politically-exposed person",
            data_source=DataSourceConfig("OSINT", "open data sources"),
            is_supporting_flag=True,
            weight=2,
            initial_assignment_prob=0.05,
        ),
        FlagConfig(
            id="F7",
            category="control",
            name="market division",
            description="The company is involved in market division",
            data_source=DataSourceConfig(
                "FININT", "data sources on money transactions"
            ),
            is_supporting_flag=False,
            weight=3,
            initial_assignment_prob=0.06,
        ),
        FlagConfig(
            id="F8",
            category="control",
            name="bid rigging",
            description="The company is involved in bid rigging",
            data_source=DataSourceConfig(
                "FININT", "data sources on money transactions"
            ),
            is_supporting_flag=False,
            weight=3,
            initial_assignment_prob=0.05,
        ),
        FlagConfig(
            id="F9",
            category="control",
            name="price fixing",
            description="The company is involved in price fixing",
            data_source=DataSourceConfig(
                "FININT", "data sources on money transactions"
            ),
            is_supporting_flag=False,
            weight=3,
            initial_assignment_prob=0.03,
        ),
    ],
    assigned_flag_boost=0.1,
    baseline_community_weight=0.2,
    min_intra_community_connection_prob=0.3,
    max_intra_community_connection_prob=0.7,
    min_inter_community_connection_prob=0.0,
    max_inter_community_connection_prob=0.01,
    related_activity_connection_prob=0.4,
    similar_activity_partition_prob=0.3,
    related_attribute_connection_prob=0.7,
    separator=":",
    locale="en_US",
)

ENTITY_TABLE: Final[str] = "entity"
CONTACT_ATTRIBUTE_TABLE: Final[str] = "contact"
OWNERSHIP_ATTRIBUTE_TABLE: Final[str] = "ownership"
DYNAMIC_ATTRIBUTE_TABLE: Final[str] = "activity"
FLAG_TABLE: Final[str] = "redflag"
FLAG_METADATA_TABLE: Final[str] = "flag_definitions"
ATTRIBUTE_METADATA_TABLE: Final[str] = "attribute_definitions"
OWNERSHIP_ATTRIBUTES: Final[List[str]] = ["beneficial_owner", "director"]


def generate_procurement_data(
    config: DataGeneratorConfig = procurement_configs,
    n_entities: int = 1000,
    n_communities: int = 20,
    n_periods: int = 20,  
) -> Dict[str, DataFrame]:
    """
    Gerenerate synthetic data for the public procurement use case.

    Params:
        output_path: str
            Path to save the synthesized data
        n_entities: int, default = 1000
            Number of entities to generate
        n_communities: int, default = 20
            Number of communities in the entity graph.
        n_periods: int, default = 20
            Number of observed periods

    Returns: Dictionary containing 7 dataframes to be used as inputs for the transparency engine model
    """
    procurement_configs.number_of_communities = n_communities
    procurement_configs.number_of_entities = n_entities
    procurement_configs.observed_periods = n_periods
    generator = SyntheticDataGenerator(config)
    data = generator.generate_data()
    contact_attributes = data.static_attributes.filter(
        ~F.col(ATTRIBUTE_ID).isin(OWNERSHIP_ATTRIBUTES)
    )
    ownership_attributes = data.static_attributes.filter(
        F.col(ATTRIBUTE_ID).isin(OWNERSHIP_ATTRIBUTES)
    )

    data_dict = {
        ENTITY_TABLE: data.entity,
        CONTACT_ATTRIBUTE_TABLE: contact_attributes,
        OWNERSHIP_ATTRIBUTE_TABLE: ownership_attributes,
        DYNAMIC_ATTRIBUTE_TABLE: data.dynamic_attributes,
        FLAG_TABLE: data.flags,
        FLAG_METADATA_TABLE: data.flag_metadata,
        ATTRIBUTE_METADATA_TABLE: data.attribute_metadata
    }
    return data_dict


