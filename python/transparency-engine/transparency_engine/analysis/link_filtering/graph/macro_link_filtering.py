#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import List

from dataclasses import dataclass, field
from pyspark.sql import DataFrame

import transparency_engine.modules.graph.link_filtering.macro_links as macro_links
import transparency_engine.pipeline.schemas as schemas

from transparency_engine.analysis.link_filtering.base_link_filter import BaseLinkFilter


logger = logging.getLogger(__name__)


@dataclass
class MacroLinkFilteringConfig:
    """
    Configuration for the MacroLinkFilter.

    Attributes:
        max_path_length: The maximum length of a path between two nodes.
        max_fuzzy_chain_length: The maximum length of a fuzzy chain between two nodes.
        max_dynamic_chain_length: The maximum length of a dynamic chain between two nodes.
        min_direct_links: The minimum number of direct links between two nodes.
        noisy_relationships: The list of noisy relationships to be filtered out.

    """

    join_token: str = "::"
    max_path_length: int = 5
    max_fuzzy_chain_length: int = 1
    max_dynamic_chain_length: int = 1
    min_direct_links: int = (
        0  # set this to 1 or higher if we want to enforce the existence of direct links
    )
    noisy_relationships: List[str] = field(default_factory=lambda: [])

class MacroLinkFilter(BaseLinkFilter):
    """
    This filter removes macro links between nodes.

    Params:
    -------
        config: The configuration for the filter.
        multipartite_tables: The list of multipartite tables.
        entity_col: The name of the entity column.
        source_col: The name of the source column.
        target_col: The name of the target column.
        related_col: The name of the related column.
        source_type_col: The name of the source type column.
        target_type_col: The name of the target type column.
    """

    def __init__(
        self,
        config: MacroLinkFilteringConfig,
        multipartite_tables: List[DataFrame],
        entity_col: str = schemas.ENTITY_ID,
        source_col: str = schemas.SOURCE,
        target_col: str = schemas.TARGET,
        related_col: str = schemas.RELATED,
        source_type_col: str = schemas.SOURCE_TYPE,
        target_type_col: str = schemas.TARGET_TYPE,
    ):
        super().__init__(config)

        self.config = config
        self.multipartite_tables = multipartite_tables
        self.entity_col = entity_col
        self.source_col = source_col
        self.target_col = target_col
        self.related_col = related_col
        self.source_type_col = source_type_col
        self.target_type_col = target_type_col

    def filter(self, input_data: DataFrame) -> DataFrame:
        """
        Filters the graph by removing macro links between nodes.

        Params:
            input_data: The predicted links.
            multipartite_graphs: The list of multipartite graphs.
            source_col: The name of the source column.
            target_col: The name of the target column.
            source_type_col: The name of the source type column.
            target_type_col: The name of the target type column.

        Returns:
            The filtered graph.
        """

        graph, edge_data = macro_links.generate_nx_graph(
            self.multipartite_tables,
            self.source_col,
            self.target_col,
            self.source_type_col,
            self.target_type_col,
            join_token=self.config.join_token,
        )

        filtered_links = macro_links.get_valid_links(
            predicted_links=input_data,
            graph=graph,
            edge_data=edge_data,
            entity_col=self.entity_col,
            source_entity=self.source_col,
            target_entity=self.target_col,
            join_token=self.config.join_token,
            noisy_relationships=self.config.noisy_relationships,
            max_path_length=self.config.max_path_length,
            max_fuzzy_chain_length=self.config.max_fuzzy_chain_length,
            max_dynamic_chain_length=self.config.max_dynamic_chain_length,
            min_direct_links=self.config.min_direct_links,
        )

        expanded_links = macro_links.add_missing_links(
            predicted_links=filtered_links,
            graph=graph,
            edge_data=edge_data,
            entity_col=self.entity_col,
            related_entities=self.related_col,
            source_entity=self.source_col,
            target_entity=self.target_col,
            join_token=self.config.join_token,
            noisy_relationships=self.config.noisy_relationships,
            max_path_length=self.config.max_path_length,
            max_fuzzy_chain_length=self.config.max_fuzzy_chain_length,
            max_dynamic_chain_length=self.config.max_dynamic_chain_length,
            min_direct_links=self.config.min_direct_links,
        )
        return expanded_links
