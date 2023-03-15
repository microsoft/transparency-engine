#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
"""
Main module for the transparency engine.

This module contains the main function for the transparency engine.

Example
-------
    $ python transparency_engine/main.py --config pipeline.json --steps steps.json
"""

import argparse
import json
import logging

from typing import Any, Dict

from transparency_engine.containers import ContainerKeys, build_container
from transparency_engine.io.data_handler import DataHandler, DataHandlerModes
from transparency_engine.pipeline import TransparencyPipeline
from transparency_engine.typing import PipelineSteps


# Set to info for now. Pull from config later
logging.basicConfig(level=logging.INFO)


def main(config_path: str, steps_path: str) -> None:
    """
    Run Main function for the transparency engine.

    Parameters
    ----------
        config_path : str
            Path to the configuration file.

    Returns
    -------
        None
    """
    # Initialize the containers
    pipeline_config: Dict[str, Any] = dict()
    step_config: Dict[str, Any] = dict()

    with open(config_path) as config_file:
        pipeline_config = json.load(config_file)

    with open(steps_path) as steps_file:
        step_config = json.load(steps_file)

    storage_config: Dict[str, str] = pipeline_config.get("storage", dict())

    build_container(
        {
            ContainerKeys.STEP_CONFIG: step_config,
            ContainerKeys.PIPELINE_CONFIG: pipeline_config,
            ContainerKeys.DATA_HANDLER: (
                DataHandler,
                DataHandlerModes.from_string(storage_config.get("type", "")),
                storage_config.get("root", ""),
            ),
        },
        modules=["transparency_engine.pipeline"],
        packages=[],
    )

    pipeline = TransparencyPipeline()

    steps = PipelineSteps.from_string_list(pipeline_config.get("steps", []))

    pipeline.run(steps=steps)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute the transparency engine.")
    parser.add_argument(
        "--config", metavar="config", required=True, help="the path to config json file"
    )
    parser.add_argument(
        "--steps", metavar="steps", required=True, help="path to the steps json file"
    )
    args = parser.parse_args()
    main(args.config, args.steps)
