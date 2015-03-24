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
    'frobenius_hashtags',
    'frobenius_counties',
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

def pipeline(hashtags, norm):
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

def frobenius_hashtags(N):
    """
    Returns the top N hashtags according to Frobenius norm on the Fisher matrix.

    """
    db = twitterproj.connect()
    collection = db.hashtags.bot_filtered

    hashtags = []
    hashtags.extend(top_hashtags(N, 'count', collection, extract=False))
    hashtags.extend(top_hashtags(N, 'user_count', collection, extract=False))

    hashtags_dict = {}
    for hashtag_doc in hashtags:
        ht = hashtag_doc['_id']
        hashtags_dict[ht] = hashtag_doc
    hashtags = list(hashtags_dict.keys())

    norms = [frobenius_norm]
    scores = []
    for norm in norms:
        scores.append(pipeline(hashtags, norm))
    scores = list(zip(*scores))

    lines = []
    for i, hashtag in enumerate(hashtags):

        line = [
            hashtag,
            hashtags_dict[hashtag]['count'],
            hashtags_dict[hashtag]['user_count'],
            scores[i][0],
        ]
        lines.append(line)

    lines.sort(key=itemgetter(3))
    return lines

def frobenius_counties(hashtags, relative=True):
    """
    Returns the county geoid, frobenius score, and counts over `hashtags`.

    Parameters
    ----------
    diffscore : bool
        If `true` the relative Frobenius score is calculated and returned.
        The ratio is calculated with respect to the maximum possible score.

    The posterior mean distribution is just the normalization of the counts.

    """
    db = twitterproj.connect()
    collection = db.grids.counties.bot_filtered

    x = hashtag_countycounts(hashtags, collection, prior=1/2)
    x = x.transpose()

    # x.shape is (counties, hashtags)
    geoids = []
    for i, doc in enumerate(twitterproj.hashtag_counts__counties()):
        geoids.append(doc['geoid'])

    if relative:
        prior = np.array([1/2 for _ in range(len(hashtags))])
        maxscore = frobenius_norm(prior)
    else:
        maxscore = 1

    scores = [frobenius_norm(counts) / maxscore for counts in x]

    out = zip(geoids, scores, x)
    return out
