#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#
"""
Container definition.

This module contains the dependency injection container for the transparency engine.
"""


from enum import Enum
from typing import Any, Dict, List

from dependency_injector import containers, providers
from dependency_injector.wiring import register_loader_containers


class ContainerKeys(str, Enum):
    """
    Container keys.

    This class contains the keys for the dependency injection container.
    """

    DATA_HANDLER = "data_handler"
    PIPELINE_CONFIG = "pipeline_config"
    STEP_CONFIG = "step_config"


def build_container(
    attributes: Dict[ContainerKeys, Any],
    modules: List[str],
    packages: List[str],
) -> containers.DynamicContainer:
    """
    Build the dependency injection container.

    Parameters
    ----------
    attributes : Dict[ContainerKeys, Any]
        The attributes to add to the container.
    modules : List[str]
        The modules to add to the container.
    packages : List[str]
        The packages to add to the container.

    Returns
    -------
    containers.DynamicContainer
        The dependency injection container.
    """
    container = containers.DynamicContainer()
    register_loader_containers(container)

    for attribute, value in attributes.items():
        if isinstance(value, Dict):
            setattr(container, attribute, providers.Dict(value))
        elif isinstance(value, List):
            setattr(container, attribute, providers.List(value))
        elif isinstance(value, tuple):
            # If it is a Tuple, we unpack and invoke the callable
            setattr(container, attribute, providers.ThreadSafeSingleton(*value))
    container.wiring_config = containers.WiringConfiguration(
        modules=modules, packages=packages
    )

    # Wire the container configuration
    container.wire()

    return container
