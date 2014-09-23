"""
Script demonstrating how to extract max and min entropic counties.

This uses the library written for this project.

Note: If you redirect output to a file in 2.x, this will fail
since the encoding will be ASCII and the text we are printing is
definitely not just ASCII. Set PYTHONIOENCODING=UTF-8 in that case.
See here for other info:
http://stackoverflow.com/questions/4545661/unicodedecodeerror-when-redirecting-to-file

"""
from __future__ import print_function
from __future__ import unicode_literals

import us
import numpy as np
import dit

# Custom library written for this project.
import twitterproj

def entropy(county):
    """
    Return the entropy of a county's hashtag counts.

    """
    pmf = np.array( county['counts'].values(), dtype=float)
    pmf /= pmf.sum()
    H = dit.shannon.entropy_pmf(pmf)
    return H

def minmax_entropies():
    """
    Print the max and min entropy for each state.

    """
    # Connect to the MongoDB database
    db = twitterproj.connect()

    states = us.STATES_CONTIGUOUS
    minmax = {}
    for state in states:
        min_cty = None
        max_cty = None
        min_H = +np.inf
        max_H = -np.inf
        print("Calculating for {}".format(state.name))
        for county in twitterproj.hashtag_counts__counties(db, state.abbr):
            H = entropy(county)
            if H < min_H:
                min_H = H
                min_cty = county
            if H > max_H:
                max_H = H
                max_cty = county

        minmax[state] = (min_cty, max_cty, min_H, max_H)

    # Now let's print it nicely.
    for state in states:
        print(state.name)
        min_cty, max_cty, min_H, max_H = minmax[state]
        print("   min: {0}{1:09.06f}".format(min_cty['name'].ljust(30), min_H))
        print("   max: {0}{1:09.06f}".format(max_cty['name'].ljust(30), max_H))

if __name__ == '__main__':
    minmax_entropies()
