"""
Script demonstrating how to extract county information from MongoDB.

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

def county_entropies(state_abbrev):
    """
    Print the entropy of the hashtag distribution for a state.

    """
    db = twitterproj.connect()

    abbr_to_name = us.states.mapping('abbr', 'name')
    fips_to_name = us.states.mapping('fips', 'name')

    text = "Counties of {}:".format(abbr_to_name[state_abbrev])
    print(text)

    out = []
    for county in twitterproj.hashtag_counts__counties(db, state_abbrev):
        H = entropy(county)
        name = county['name'].ljust(30)
        text = "\t{0}{1:09.06f}".format(name, H)
        out.append(text)

    out.sort()
    print('\n'.join(out))

if __name__ == '__main__':
    county_entropies('WA')
