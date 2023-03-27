#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import Any, Dict, List, Union

from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

import transparency_engine.analysis.scoring.entity_scoring as entity_scoring
import transparency_engine.analysis.scoring.network_scoring as network_scoring
import transparency_engine.pipeline.schemas as schemas
import transparency_engine.reporting.entity_report as entity_report

from transparency_engine.analysis.link_filtering.graph.dynamic_individual_link_filtering import (
    DynamicIndividualLinkFilter,
    DynamicLinkFilteringConfig,
)
from transparency_engine.analysis.link_filtering.graph.macro_link_filtering import (
    MacroLinkFilter,
    MacroLinkFilteringConfig,
)
from transparency_engine.analysis.link_inference.dynamic_link_estimator import (
    USEDynamicLinkConfig,
    USEDynamicLinkEstimator,
)
from transparency_engine.analysis.link_inference.macro_link_estimator import (
    USEMacroLinkEstimator,
)
from transparency_engine.analysis.link_inference.static_link_estimator import (
    USEStaticLinkConfig,
    USEStaticLinkEstimator,
)
from transparency_engine.containers import ContainerKeys
from transparency_engine.io.data_handler import DataHandler, DataHandlerModes
from transparency_engine.preprocessing.flag.flag_filtering import (
    FlagFilterConfig,
    FlagFilterTransformer,
    generate_flag_metadata,
    validate_flag_metadata,
)
from transparency_engine.preprocessing.graph.multipartite_graph import (
    MultipartiteGraphConfig,
    MultipartiteGraphTransformer,
)
from transparency_engine.preprocessing.graph.multiplex_graph import (
    MultiplexGraphConfig,
    MultiplexGraphTransformer,
)
from transparency_engine.preprocessing.text.lsh_fuzzy_matching import (
    LSHConfig,
    LSHFuzzyMatchTransformer,
)
from transparency_engine.typing import InputLoadTypes, PipelineSteps


logger = logging.getLogger(__name__)


class TransparencyPipeline:
    """
    Base class for all pipelines in the Transparency Engine.
    """

    config: Dict[str, Any] = Provide[ContainerKeys.PIPELINE_CONFIG]
    step_config: Dict[str, Any] = Provide[ContainerKeys.STEP_CONFIG]
    data_handler: DataHandler = Provide[ContainerKeys.DATA_HANDLER]

    @inject
    def __init__(self):
        """
        Initialize the pipeline.
        """

    def run(
        self,
        steps: List[PipelineSteps] = [PipelineSteps.ALL],
    ) -> None:
        """
        Run the pipeline.
        """
        logger.info("Initializing pipeline")

        for step in steps:
            if step == PipelineSteps.DATA_PREP:
                self.data_prep()
            elif step == PipelineSteps.IND_LINK_PREDICTION:
                self.individual_link_prediction()
            elif step == PipelineSteps.IND_LINK_FILTERING:
                self.individual_link_filtering()
            elif step == PipelineSteps.MACRO_LINK_PREDICTION:
                self.macro_link_prediction()
            elif step == PipelineSteps.MACRO_LINK_FILTERING:
                self.macro_link_filtering()
            elif step == PipelineSteps.SCORING:
                self.scoring()
            elif step == PipelineSteps.REPORT:
                self.report()
            elif step == PipelineSteps.ALL:
                self.data_prep()
                self.individual_link_prediction()
                self.individual_link_filtering()
                self.macro_link_prediction()
                self.macro_link_filtering()
                self.scoring()
                self.report()
            else:
                raise ValueError(f"Unknown step: {step}")

    def data_prep(self) -> None:
        """
        Run the data prep step.
        """

        logger.info("Running data prep step")
        for prep_step in self.step_config.get("steps", {}).get(
            PipelineSteps.DATA_PREP, [{}]
        ):
            input_name = prep_step.get("name", "")
            input_path = prep_step.get("path", "")
            input_type = InputLoadTypes.from_string(prep_step.get("type", ""))

            logger.info(
                f"Running data prep step: {input_name} ({input_type.name}) [{input_type.schema}]"
            )
            load_data_handler = DataHandler(DataHandlerModes.CSV)
            result_df: Union[DataFrame, None] = None
            fuzzy_df: Union[DataFrame, None] = None
            for substep in prep_step.get("steps", []):
                if substep == "load":

                    logger.info(f"Loading: {input_path}")
                    result_df = load_data_handler.load_data(
                        input_path, mode=DataHandlerModes.CSV, schema=input_type.schema
                    )

                    # Save dataframe in current working directory
                    self.data_handler.write_data(result_df, input_name)

                elif substep == "fuzzy_match" and result_df is not None:
                    # Using default config
                    fuzzy_config = {
                        attr: LSHConfig()
                        for attr in prep_step.get("fuzzy_match_on", [])
                    }
                    fuzzy_df = LSHFuzzyMatchTransformer(fuzzy_config).transform(
                        result_df
                    )

                elif substep == "preprocess" and result_df is not None:
                    result_df = self._preprocess(
                        result_df=result_df,
                        input_type=input_type,
                        input_name=input_name,
                        prep_step=prep_step,
                        fuzzy_matching_df=fuzzy_df,
                        load_data_handler=load_data_handler,
                    )

            if result_df is not None:
                self.data_handler.write_data(result_df, f"{input_name}_prep")

    def individual_link_prediction(self) -> None:
        """
        Run the graph individual linking step.
        """
        logger.info("Running individual link prediction step")
        linking_steps = self.step_config.get("steps", {}).get(
            PipelineSteps.IND_LINK_PREDICTION, {}
        )

        for static_input in linking_steps.get("static"):
            input_name = static_input.get("name", "")
            prep_df = self.data_handler.load_data(f"{input_name}_prep")

            static_config = USEStaticLinkConfig()
            links_df = USEStaticLinkEstimator(static_config, input_name).predict(
                prep_df
            )

            self.data_handler.write_data(links_df, f"{input_name}_links")

        for dynamic_input in linking_steps.get("dynamic"):
            input_name = dynamic_input.get("name", "")
            prep_df = self.data_handler.load_data(f"{input_name}_prep")

            dynamic_config = USEDynamicLinkConfig()
            links_df = USEDynamicLinkEstimator(dynamic_config, input_name).predict(
                prep_df
            )

            self.data_handler.write_data(links_df, f"{input_name}_links")

    def individual_link_filtering(self) -> None:
        """
        Run the graph link filtering step.
        """
        logger.info("Running individual link filtering step")
        filtering_steps = self.step_config.get("steps", {}).get(
            PipelineSteps.IND_LINK_FILTERING, {}
        )

        for dynamic_input in filtering_steps.get("dynamic"):
            input_name = dynamic_input.get("name", "")
            links_df = self.data_handler.load_data(f"{input_name}_links")
            multipartite_df = self.data_handler.load_data(f"{input_name}_prep")

            async_attributes = dynamic_input.get("config", {}).get(
                "async_attributes", []
            )
            sync_attributes = dynamic_input.get("config", {}).get("sync_attributes", [])

            config = DynamicLinkFilteringConfig()
            config.async_attributes = async_attributes
            config.sync_attributes = sync_attributes

            filtered_links_df, multipartite_filtered_df = DynamicIndividualLinkFilter(
                data_type=input_name,
                config=config,
                multipartite_graph_input=multipartite_df,
            ).filter(links_df)

            filtered_links_df.cache().show()
            logger.info(f"Final link count: {filtered_links_df.count()}")
            self.data_handler.write_data(
                filtered_links_df, f"{input_name}_filtered_links"
            )

            self.data_handler.write_data(
                multipartite_filtered_df, f"{input_name}_filtered_graph"
            )

    def macro_link_prediction(self) -> None:
        """
        Run the graph macro linking step.
        """
        logger.info("Running macro link prediction step")
        linking_step = self.step_config.get("steps", {}).get(
            PipelineSteps.MACRO_LINK_PREDICTION, {}
        )

        input_name = linking_step.get("name", "")
        dataframes_list = [
            self.data_handler.load_data(input_name)
            for input_name in linking_step.get("inputs", [])
        ]

        macro_config = USEStaticLinkConfig()
        macro_links_df = USEMacroLinkEstimator(configs=macro_config).predict(
            dataframes_list
        )

        self.data_handler.write_data(macro_links_df, f"{input_name}_links")

    def macro_link_filtering(self) -> None:
        """
        Run the graph link filtering step.
        """
        logger.info("Running macro link filtering step")
        filtering_step = self.step_config.get("steps", {}).get(
            PipelineSteps.MACRO_LINK_FILTERING, {}
        )
        input_name = filtering_step.get("name", "")

        macro_links_df = self.data_handler.load_data(f"{input_name}_links")
        static_dataframes_list = [
            self._to_multipartite(self.data_handler.load_data(input.get("name", "")))
            for input in filtering_step.get("static", [{}])
        ]

        dynamic_dataframes_list = [
            self.data_handler.load_data(f"{input.get('name', '')}_filtered_graph")
            for input in filtering_step.get("dynamic", [{}])
        ]

        dataframes_list = static_dataframes_list + dynamic_dataframes_list

        macro_config = MacroLinkFilteringConfig()
        macro_filtered_links_df = MacroLinkFilter(
            config=macro_config, multipartite_tables=dataframes_list
        ).filter(macro_links_df)
        macro_filtered_links_df.show()
        self.data_handler.write_data(
            macro_filtered_links_df, f"{input_name}_filtered_links"
        )

    def scoring(self) -> None:
        """
        Run the scoring step.
        """
        logger.info("Running scoring step")

        scoring_step = self.step_config.get("steps", {}).get(PipelineSteps.SCORING, {})

        entity_data_df = self.data_handler.load_data(
            f"{scoring_step.get('entity', '')}_prep"
        )
        entity_flag_df = self.data_handler.load_data(
            f"{scoring_step.get('entity_flag', '')}_prep"
        )
        flag_metadata_df = self.data_handler.load_data(
            scoring_step.get("flag_metadata", "")
        )
        predicted_links_df = self.data_handler.load_data(
            f"{scoring_step.get('predicted_links', '')}_filtered_links"
        )

        entity_score_df = entity_scoring.compute_entity_score(
            entity_data_df, entity_flag_df, flag_metadata_df
        ).cache()
        logger.info(f"Finished computing entity score: {entity_score_df.count()}")
        entity_score_df.show(5)

        network_score_df = network_scoring.compute_network_score(
            entity_score_df, predicted_links_df
        )
        logger.info(f"Finished computing network score")
        network_score_df.show(5)

        self.data_handler.write_data(entity_score_df, "entity_scoring")
        self.data_handler.write_data(network_score_df, "network_scoring")

    def report(self) -> None:
        """
        Run the report step.
        """
        logger.info("Running report step")

        report_step = self.step_config.get("steps", {}).get(PipelineSteps.REPORT, {})

        # Load all dfs
        entity_data_df = self.data_handler.load_data(
            f"{report_step.get('entity', '')}_prep"
        )
        static_dfs = [
            self.data_handler.load_data(f'{input.get("name", "")}')
            for input in report_step.get("static", [{}])
        ]
        dynamic_dfs = [
            self.data_handler.load_data(f"{input.get('name', '')}_prep")
            for input in report_step.get("dynamic", [{}])
        ]

        other_dfs = [
            self.data_handler.load_data(f'{input.get("name", "")}')
            for input in report_step.get("other", [])
        ]

        entity_flag_df = self.data_handler.load_data(
            f"{report_step.get('entity_flag', '')}_prep"
        )
        network_score_df = self.data_handler.load_data("network_scoring")
        predicted_links_df = self.data_handler.load_data(
            f"{report_step.get('predicted_links', '')}_filtered_links"
        )
        flag_metadata_df = self.data_handler.load_data(
            report_step.get("flag_metadata", "")
        )
        attribute_metadata_df = self.data_handler.load_data(
            f"{report_step.get('attribute_metadata', '')}_prep"
        )

        sync_attributes = report_step.get("config", {}).get("sync_attributes", [])
        async_attributes = report_step.get("config", {}).get("async_attributes", [])
        entity_name_attribute = report_step.get("config", {}).get(
            "entity_name_attribute", []
        )

        # Run the report
        config = entity_report.ReportConfig(
            sync_link_attributes=sync_attributes,
            async_link_attributes=async_attributes,
            entity_name_attribute=entity_name_attribute,
        )

        report_output = entity_report.generate_report(
            entity_data=entity_data_df,
            static_relationship_data=static_dfs,
            dynamic_graph_data=dynamic_dfs,
            other_attribute_data=other_dfs,
            entity_flag_data=entity_flag_df,
            network_score_data=network_score_df,
            predicted_link_data=predicted_links_df,
            flag_metadata=flag_metadata_df,
            attribute_metadata=attribute_metadata_df,
            configs=config,
            entity_name_attribute=entity_name_attribute,
        )

        self.data_handler.write_data(
            report_output.entity_activity_report.entity_activity,
            "entity_activity_report",
        )
        self.data_handler.write_data(
            report_output.entity_activity_report.entity_activity_summary_scores,
            "entity_temporal_activity_report",
        )
        self.data_handler.write_data(
            report_output.entity_activity_report.entity_link_temporal_scores,
            "entity_related_activity_report",
        )
        self.data_handler.write_data(
            report_output.entity_activity_report.entity_link_overall_scores,
            "entity_related_activity_overall_report",
        )
        self.data_handler.write_data(
            report_output.entity_activity_report.entity_link_counts,
            "entity_activity_link_report",
        )
        self.data_handler.write_data(
            report_output.entity_attribute_report.entity_attributes,
            "entity_attributes_report",
        )
        self.data_handler.write_data(report_output.entity_graph, "entity_graph_report")
        self.data_handler.write_data(report_output.html_report, "html_report")

    def get_config(self) -> Dict[str, Any]:
        """
        Get the configuration for the pipeline.

        Returns
        -------
        PipelineConfig
            The configuration for the pipeline.
        """
        return self.config

    def _preprocess(
        self,
        result_df: DataFrame,
        input_type: InputLoadTypes,
        input_name: str,
        prep_step: Dict[str, Any],
        fuzzy_matching_df: Union[DataFrame, None],
        load_data_handler: DataHandler,
    ) -> Union[DataFrame, None]:
        """
        Run the preprocess substep.

        Params:
            result_df: DataFrame
                Result of the previous substep
            input_type: InputLoadTypes
                Type of input data
            input_name: str
                Name of input data
            prep_step: Dict[str, Any]
                Configuration of the preprocess step
            fuzzy_matching_df: Union[DataFrame, None]
                Results of the fuzzy matching substep
            load_data_handler: DataHandler
                Data handler object

        Returns:
            results_df: DataFrame Results of the preprocess substep
        """
        if input_type.name == InputLoadTypes.Dynamic.name:
            # build multipartite graph
            logger.info("Building multipartite graph")
            result_df = self._to_multipartite(result_df, fuzzy_matching_df)

        elif input_type.name == InputLoadTypes.Static.name:
            # build multiplex graph
            logger.info("Building multiplex graph")
            attributes = [
                v[schemas.ATTRIBUTE_ID]
                for v in result_df.select(schemas.ATTRIBUTE_ID).distinct().collect()
            ]
            multiplex_config = {attr: MultiplexGraphConfig() for attr in attributes}

            result_df = MultiplexGraphTransformer(
                multiplex_config, fuzzy_matching_df
            ).transform(result_df)
        elif input_type.name == InputLoadTypes.ReviewFlag.name:
            # build flag metadata and filter invalid flags if needed
            flag_metadata_config = prep_step.get("metadata", {})
            flag_input_path = flag_metadata_config.get("path", "")
            flag_input_type = InputLoadTypes.from_string(
                flag_metadata_config.get("type", "")
            )
            if flag_input_path != "":
                logger.info(f"Loading: {flag_input_path}")
                flag_metadata = load_data_handler.load_data(
                    flag_input_path,
                    mode=DataHandlerModes.CSV,
                    schema=flag_input_type.schema,
                )
                flag_metadata = validate_flag_metadata(flag_metadata)

                # filter invalid flags
                logger.info("Filtering invalid flags")
                flag_config = FlagFilterConfig()
                result_df = FlagFilterTransformer(flag_config, flag_metadata).transform(
                    result_df
                )
            else:
                flag_metadata = generate_flag_metadata(result_df)
            self.data_handler.write_data(flag_metadata, f"{input_name}_metadata")
        return result_df

    def _to_multipartite(
        self, result_df: DataFrame, fuzzy_matching_df: Union[DataFrame, None] = None
    ) -> DataFrame:
        """
        Transform the dataframe to a multipartite graph.

        Params:
        -------
            result_df: DataFrame
                Result of the previous substep
            fuzzy_matching_df: Union[DataFrame, None]
                Results of the fuzzy matching substep

        Returns:
        -------
            results_df: DataFrame Results of the preprocess substep"""
        attributes = [
            v[schemas.ATTRIBUTE_ID]
            for v in result_df.select(schemas.ATTRIBUTE_ID).distinct().collect()
        ]
        multipartite_config = {attr: MultipartiteGraphConfig() for attr in attributes}

        result_df = MultipartiteGraphTransformer(
            multipartite_config, fuzzy_matching_df
        ).transform(result_df)

        return result_df
