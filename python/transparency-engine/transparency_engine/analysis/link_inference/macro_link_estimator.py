#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from functools import reduce
from typing import List, Union

from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)

from transparency_engine.analysis.link_inference.static_link_estimator import (
    USEStaticLinkConfig,
    USEStaticLinkEstimator,
)


class USEMacroLinkEstimator(USEStaticLinkEstimator):
    """
    This estimator takes as input all the links predicted from the individual static or dynamic relationships,
    and uses the links as edges of a multiplex graph. It then computes the anchor embedding
    for each entity in the new multiplex graph using the unfolded spectral embedding method. The anchor embedding reperesents
    the entity's overall behavior across all static and dynamic relationshps, and is then used to compute N nearest
    neighbors for each entity in the embedding space.
    """

    def __init__(
        self,
        configs: USEStaticLinkConfig,
        source_col: str = "Source",
        target_col: str = "Target",
        source_type_col: str = "SourceType",
        target_type_col: str = "TargetType",
        relationship_col: str = "Relationship",
        weight_col: str = "Weight",
    ):
        """
        Initializes the estimator.

        Params:
        -------------
        configs: USEStaticLinkConfig
            Configuations of the estimator
        source_col: str default = 'Source'
            Name of the column that contains the source entity node
        target_col: str default = 'Target'
            Name of the column that contains the target entity node
        source_type_col: str default = 'SourceType'
            Name of the column that contains the type (i.e. partition) of the Source node.
        target_type_col: str default = 'TargetType'
            Name of the column that contains the type (i.e. partition) of the Target node.
        unfold_attribute_col: default = 'Relationship'
            Name of the column that contains the entity static attributes used for unfolding the graph. (e.g. name/address)
            For dynamic graph, this column could contain the time periods.
         weight_col: default = 'Weight'
            Name of the column that contains edge weights
        """
        super().__init__(
            configs=configs,
            relationship_family="macro",
            source_col=source_col,
            target_col=target_col,
            source_type_col=source_type_col,
            target_type_col=target_type_col,
            relationship_col=relationship_col,
            weight_col=weight_col,
        )

    def predict(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        """
        Predict entity links using

        Params:
            input_data: Union[DataFrame, List[DataFrame]]
                Contains the entity-entity links predicted using individual static/dynamic relationships.
                Each dataframe has a schema [source_col, target_col, source_type_col, target_type_col, relationship_col, weight_col]

        Returns:
            Spark DataFrame Dataframe containing the predicted links with format [Source, Target, SourceType, TargetType, Weight]
        """
        if isinstance(input_data, List):
            cleaned_input_data = []
            for data_part in input_data:
                data_part = data_part.select(
                    self.source_col,
                    self.target_col,
                    self.weight_col,
                    self.source_type_col,
                    self.target_type_col,
                    self.relationship_col,
                )
                cleaned_input_data.append(data_part)
            input_data = reduce(DataFrame.unionAll, cleaned_input_data)

        predicted_link_df = super().predict(input_data)
        predicted_link_df = predicted_link_df.drop(self.relationship_col)
        return predicted_link_df
