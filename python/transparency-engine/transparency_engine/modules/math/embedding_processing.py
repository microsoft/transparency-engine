#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from typing import Union

import numpy as np

from kneed import KneeLocator


logger = logging.getLogger(__name__)


def degree_correct(embedding_vectors: np.ndarray, tol: float = 1e-12):
    """
    Perform in-place degree correction on a set of embedding vectors

    Parameters
    ----------
        embedding_vectors: ndarray
            Embedding vectors
        tol: float, default = 1e-12
            Tolerance threshold
    """
    norms = np.linalg.norm(embedding_vectors, axis=1)
    idx = np.where(norms > tol)
    embedding_vectors[idx] = embedding_vectors[idx] / (norms[idx, None])


def truncate(
    embedding_vectors: np.ndarray,
    embedding_dim: Union[int, None] = None,
    singular_values: Union[np.ndarray, None] = None,
    elbow_sensitivity: float = 2.0,
) -> np.ndarray:
    """
    Truncate the original embedding to a given embedding dimension.
    If no embedding size is specified, automatically find an optimal embedding dimension based on the elbow test

    Parameters
    ----------
        embedding_vectors: ndarray
            Embedding vectors
        embedding_dim: int, default = None
            Number of dimensions to truncate the embeddings to. If None, automatically find an optimal embedding dimension based on the elbow test
        elbow_sensitivity: float, default = 2.0
            Sensitivity of the elbow test

    Returns:
    ----------
        embedding_vectors: ndarray
            Truncated embeddings
    """
    if embedding_dim is None:
        # automatically find optimal embedding size based on elbow in the scree plot
        embedding_dim = __find_elbow(singular_values, elbow_sensitivity)
        if embedding_dim < 2:
            embedding_dim = 2
    embedding_vectors = embedding_vectors[:, :embedding_dim]
    return embedding_vectors


def __find_elbow(singular_values, sensitivity: float = 2.0) -> int:
    """
    Find an optimal embedding dimension based on elbows in the scree plot of singular values
    """
    try:
        x = range(len(singular_values))
        y = np.sort(singular_values)[::-1]
        kneedle = KneeLocator(
            x, y, S=sensitivity, curve="convex", direction="decreasing"
        )
        return int(kneedle.elbow)
    except Exception as e:
        logger.warning(
            f"Unable to find elbow. Using the length of singular values as embedding dimension: {e}"
        )
        return len(singular_values)
