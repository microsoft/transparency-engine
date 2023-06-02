#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

from copy import deepcopy
from math import sqrt
from typing import List, Tuple, Union

import faiss
import numpy as np


logger = logging.getLogger(__name__)


def get_all_closet_pairs(
    vectors: np.ndarray,
    n_list: Union[int, None] = None,
    normalize: bool = True,
    n_probe: int = 10,
    n_neighbors: int = 20,
    min_similarity: float = 0.5,
) -> List[Tuple[int, int, float]]:
    """
    For each vector in the vector set, find N nearest neighbors within the vector set using either cosine similarity or inner product
    and approximate nearest neighbor search, then apply a distance threshold to further narrow down the returned neighbors.

    Params:
    ---------
        vectors: np.ndarray
            [m, n] array containing a set of vectors to index (m = number of vectors, n = vector dimension)
        n_list: int, default = None
            The number of cells to divide vectors to. If None, automatically select the cell number based on the number of vectors
        normalize: bool, default = True
            If True, perform L2 normalization on the vectors (i.e. using cosine similarity as the distance measure)
        n_probe: int, default = 5
            Number of nearby cells to search for neighest neighbors
        n_neighbors: int, default = 20
            Maximumn number of nearest neighbors to return for each vector
        min_similarity: float, default = 0.5
            Filter out closest pairs with similarity score < min_similarity

    Returns:
    ---------
        links: List[Tuple[int, int, float]]
            Indices of the closest pairs and the similarity scores
    """
    if vectors.ndim != 2:
        raise ValueError("Vectors to be indexed should be a 2d array")
    else:
        if vectors.shape[0] == 0:
            return []
        
        # build IVF index for embedding
        if n_list is None:
            n_vectors = vectors.shape[0]
            if n_vectors < 1000:
                n_list = 1
            else:
                n_list = int(sqrt(n_vectors))
        index = index_vectors_ivf(vectors=vectors, n_list=n_list, normalize=normalize)
        index.nprobe = min(n_probe, n_list, len(vectors))

        # find nearest neighbors for each vector in the vector set
        closest_pairs = []
        for query_index in range(vectors.shape[0]):
            query = np.array([deepcopy(vectors[query_index, :])]).astype(np.float32)
            neighbors = get_nearest_neigbors(
                query_vector=query,
                vector_index=index,
                normalize=normalize,
                n_neighbors=n_neighbors + 1,
                min_similarity=min_similarity,
            )
            for neighbor in neighbors:
                if query_index != neighbor[0]:
                    closest_pairs.append((query_index, neighbor[0], neighbor[1]))

        # sort by similarity score
        closest_pairs.sort(key=lambda x: x[2], reverse=True)
        return closest_pairs


def get_nearest_neigbors(
    query_vector: np.ndarray,
    vector_index: faiss.IndexFlatIP,
    normalize: bool = True,
    n_probe: Union[int, None] = None,
    n_neighbors: int = 20,
    min_similarity: float = 0.5,
) -> List[Tuple[int, float]]:
    """
    For a given query vector, find N nearest neighbors using  either cosine similarity or inner product
    and approximate nearest neighbor search, then apply a distance threshold to further narrow down the returned neighbors.

    Params:
    ---------
        query_vector: np.ndarray
            An 1-D vector that we want to find nearest neighbors for
        vector_index: faiss.IndexFlatIP
            The indexed vector space to perform ANN search
        normalize: bool, default = True
            If True, perform L2 normalization on the query vector (i.e. using cosine similarity as the distance measure)
        n_probe: int, default = None
            Number of nearby cells to search for neighest neighbors. If None, use default nprobe value from the vector_index object
        n_neighbors: int, default = 20
            Maximumn number of nearest neighbors to return for each vector
        min_similarity: float, default = 0.5
            Filter out closest pairs with similarity score < min_similarity

    Returns:
    ---------
        neighbors: List[Tuple[int, float]]
            Indices and similarity scores of the nearest neighbors
    """
    if n_probe is not None:
        vector_index.nprobe = min(n_probe, vector_index.nlist)

    if normalize:
        faiss.normalize_L2(query_vector)

    # find nearest neighbors for the given vector
    neighbors = []
    similarity_scores, neighbor_indices = vector_index.search(query_vector, n_neighbors)
    for similarity_score, neighbor_index in zip(
        similarity_scores[0], neighbor_indices[0]
    ):
        similarity_score = min(similarity_score, 1.0)
        if similarity_score >= min_similarity:
            neighbors.append((neighbor_index, float(similarity_score)))

    # sort by similarity score
    neighbors.sort(key=lambda x: x[1], reverse=True)
    return neighbors


def index_vectors_ivf(
    vectors: np.ndarray, n_list: int, normalize: bool = True
) -> faiss.IndexFlatIP:
    """
    Index vectors (e.g. embeddings) using inverted file (IVF) for approximate nearest neighbor (ANN) search.

    Params:
    ---------
        vectors: np.ndarray
            [m, n] array containing a set of vectors to index (m = number of vectors, n = vector dimension)
        n_list: int
            The number of cells to divide vectors to. If None, automatically select the cell number based on the number of vectors
        normalize: bool, default = True
            If True, perform L2 normalization on the vectors

    Returns:
    ---------
        index: faiss.IndexFlatIP
            The trained vector index
    """
    vectors = vectors.astype(np.float32)
    dimension = vectors.shape[1]

    if n_list is None:
        n_list = int(4 * sqrt(vectors.shape[0]))

    # normalize vectors if required (e.g. for cosine similarity)
    if normalize:
        faiss.normalize_L2(vectors)

    # build IVF index
    quantizer = faiss.IndexFlatIP(dimension)
    index = faiss.IndexIVFFlat(quantizer, dimension, n_list, faiss.METRIC_INNER_PRODUCT)
    index.train(vectors)
    index.add_with_ids(vectors, np.array(range(0, vectors.shape[0])))
    logger.info(f"Finished building IVF Index of size: {index.ntotal}")
    return index
