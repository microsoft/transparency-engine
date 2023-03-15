#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

from typing import List, Union

import numpy as np

from scipy import sparse
from scipy.sparse import csr_matrix, hstack
from scipy.sparse.linalg import norm


def safe_inv_sqrt(a: csr_matrix, tol: float = 1e-12):
    """
    Return the element-wise inverse square root of a sparse matrix with added error handling

    Parameters
    ----------
    a: sparse matrix
        The matrix whose element-wise inverse square root is to be computed
    tol: float
        A tolerance value for error handling

    Returns
    -------
    b: sparse matrix
        The element-wise inverse square root of a
    """
    with np.errstate(divide="ignore"):
        b = 1 / np.sqrt(a)
    b[np.isinf(b)] = 0
    b[a < tol] = 0
    return b


def safe_inv(a: csr_matrix, tol: float = 1e-12):
    """
    Return the element-wise inverse of a sparse matrix with added error handling

    Parameters
    ----------
    a: sparse matrix
        The matrix whose element-wise inverse is to be computed
    tol: float
        A tolerance value for error handling

    Returns
    -------
    b: sparse matrix
        The element-wise inverse of a
    """
    with np.errstate(divide="ignore"):
        b = 1 / a
    b[np.isinf(b)] = 0
    b[a < tol] = 0
    return b


def safe_div(a1: csr_matrix, a2: csr_matrix, tol: float = 1e-12):
    """
    Return the element-wise division of two sparse matrices with added error handling

    Parameters
    ----------
    a1: sparse matrix
        The first matrix
    a2: sparse matrix
        The second matrix
    tol: float
        A tolerance value for error handling

    Returns
    -------
    b: sparse matrix
        The element-wise division of a1 and a2
    """
    with np.errstate(divide="ignore"):
        b = a1 / a2
    b[np.isinf(b)] = 0
    b[a2 < tol] = 0
    return b


def zero_matrix(m: int, n: Union[int, None] = None):
    """
    Return a sparse matrix of shape (m, n) filled with zeroes

    Parameters
    ----------
    m: int
        The number of rows
    n: int, optional
        The number of columns. If not provided, it is set to m.

    Returns
    -------
    M: sparse matrix
        A sparse matrix of shape (m, n) filled with zeroes
    """
    if n is None:
        n = m
    M = sparse.coo_matrix(([], ([], [])), shape=(m, n))
    return M


def symmetric_dilation(M: csr_matrix):
    """
    Return the symmetric dilation of a sparse matrix

    Parameters
    ----------
    M: sparse matrix
        The matrix whose symmetric dilation is to be computed

    Returns
    -------
    D: sparse matrix
        The symmetric dilation of M
    """
    m, n = M.shape
    D = sparse.vstack(
        [sparse.hstack([zero_matrix(m), M]), sparse.hstack([M.T, zero_matrix(n)])]
    )
    return D


def to_laplacian(A: csr_matrix, regularizer: Union[float, None] = None) -> csr_matrix:
    """
    Return the laplacian matrix of a sparse matrix

    Parameters
    ----------
    A : sparse matrix
        The matrix whose laplacian is to be computed
    regularizer : float, optional
        A value to be added to the diagonal elements of the degree matrices. Defaults to 0.

    Returns
    -------
    L : sparse matrix
        The Laplacian matrix of A
    """
    # Compute the element-wise sum of rows and columns of A
    left_degrees = np.reshape(np.asarray(A.sum(axis=1)), (-1,))
    right_degrees = np.reshape(np.asarray(A.sum(axis=0)), (-1,))

    # if regularizer is set to 'auto', set it to the mean of the degrees
    if regularizer is None:
        regularizer = np.mean(np.concatenate((left_degrees, right_degrees)))

    # Compute the element-wise square root of the degrees and add regularizer
    left_degrees_inv_sqrt = safe_inv_sqrt(left_degrees + regularizer)
    right_degrees_inv_sqrt = safe_inv_sqrt(right_degrees + regularizer)

    # Compute the Laplacian by element-wise multiplying with square roots of degrees
    L = sparse.diags(left_degrees_inv_sqrt) @ A @ sparse.diags(right_degrees_inv_sqrt)
    return L


def reweight_matrices_by_norm(matrices: List[csr_matrix]) -> csr_matrix:
    """
    Reweight the graph using Frobenius norm

    Parameters
    ----------
        matrices: List
            List of adjacency matrices to reweight

    Returns
    -------
        reweighted_matrix: csr_matrix
            A sparse matrix that concatenates all reweighted matrices
    """
    norms = np.array([norm(m, "fro") for m in matrices])
    total_norm = np.sum(norms)
    reweight = total_norm / (len(norms) * norms)
    new_matrices = [matrices[i].multiply(reweight[i]) for i in range(len(matrices))]
    reweighted_matrix = hstack(new_matrices).tocsr()
    return reweighted_matrix
