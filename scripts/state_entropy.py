"""
Print the entropy of the hashtag distribution for each state.

"""
from __future__ import print_function

from collections import defaultdict

import numpy as np
import dit
import twitterproj

def main():
    db = twitterproj.connect()
    counts_all = defaultdict(int)
    print("State".ljust(20),
          "Entropy".rjust(12),
          "Hashtags".rjust(12),
          "Distinct Hashtags".rjust(18),
          "Distinct Ratio".rjust(18))
    for state in twitterproj.hashtag_counts__states(db):
        for ht, count in state['counts'].items():
            counts_all[ht] += count
        counts = np.array(state['counts'].values(), dtype=float)
        z = counts.sum()
        H = dit.shannon.entropy_pmf(counts / z)
        Hstr = '{}'.format(np.around(H,5))
        if len(Hstr) < 8:
            # Hack but see:
            # http://stackoverflow.com/questions/2891790/pretty-printing-of-numpy-array
            Hstr = Hstr + '0' * (8 - len(Hstr))
        vals = [state['name'].ljust(20),
                Hstr.rjust(12),
                "{:,}".format(int(z)).rjust(12),
                "{:,}".format(len(counts)).rjust(18),
                "{:.5f}".format(len(counts) / z).rjust(18)]

        print(*vals)

    print()
    counts_all = np.array(counts_all.values(), dtype=float)
    z = counts_all.sum()
    counts_all /= z
    H = dit.shannon.entropy_pmf(counts_all)
    Hstr = '{}'.format(np.around(H,5))
    vals = ["US".ljust(20),
            Hstr.rjust(12),
            "{:,}".format(int(z)).rjust(12),
            "{:,}".format(len(counts_all)).rjust(18),
            "{:.5f}".format(len(counts) / z).rjust(18)]
    print(*vals)

if __name__ == '__main__':
    main()
