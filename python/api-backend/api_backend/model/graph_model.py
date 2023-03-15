#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from dataclasses import dataclass
from typing import List


@dataclass
class Node:
    id: int
    name: str
    color: str
    labelColor: str
    nodeSize: int


@dataclass
class Edge:
    source: int
    target: int
    widthWeight: int
    targetColor: int


@dataclass
class Graph:
    nodes: List[Node]
    edges: List[Edge]
