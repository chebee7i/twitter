"""
Fisher information matrix of a Dirichlet distribution.

"""
from __future__ import division

import numpy as np

import scipy
from scipy.special import polygamma

import time

import twitterproj
import io
from operator import itemgetter

__all__ = [
    'fisher_information',
    'hashtag_countycounts',
    'top_hashtags',
    'operator_norm',
    'frobenius_norm',
]

def fisher_information(counts):
    """
    Calculates the Fisher information matrix of a Dirichlet distribution.

    Parameters
    ----------
    counts : array-like, shape (n,)
        The counts for the Dirichlet distribution.

    Returns
    -------
    fim : array-like, shape (n, n)
        The Fisher information matrix.

    """
    counts = np.asarray(counts)
    fim = np.diag(polygamma(1, counts)) - polygamma(1, counts.sum())
    return fim

def hashtag_countycounts(hashtags, collection, prior=1/2):
    """
    Return the counts of a hashtag for each county.

    """
    total = collection.find().count()
    counts = np.zeros((total, len(hashtags))) + prior
    for i, doc in enumerate(twitterproj.hashtag_counts__counties()):
        row = np.array([doc['counts'].get(ht, 0) for ht in hashtags])
        counts[i] += row

    return counts.transpose()

def top_hashtags(n, sortkey, collection, extract=True):
    docs = list(collection.find().sort(sortkey, -1).limit(n))
    if extract:
        ht = [doc['_id'] for doc in docs]
    else:
        ht = docs
    return ht

def pipline(N, hashtags, norm):
    db = twitterproj.connect()
    collection = db.grids.counties.bot_filtered

    x = hashtag_countycounts(hashtags, collection)
    scores = [norm(x[i]) for i in range(len(x))]

    return scores

def operator_norm(counts):
    M = fisher_information(counts)
    largest_eig = (M.shape[0] - 1, M.shape[0] - 1)
    eigs = scipy.linalg.eigh(M, eigvals_only=True, eigvals=largest_eig)
    return eigs[0]

def frobenius_norm(counts):
    n = len(counts)
    pgsum = polygamma(1, counts.sum())
    A = (n**2 - n) * pgsum**2
    B = polygamma(1, counts) - polygamma(1, counts.sum())
    B = (B**2).sum()
    return np.sqrt(A + B)
