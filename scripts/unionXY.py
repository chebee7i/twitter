import twitterproj
import json
import numpy as np


db = twitterproj.connect()
region_iters = [
    (twitterproj.hashtag_counts__states, 'states'),
    (twitterproj.hashtag_counts__counties, 'counties'),
    (twitterproj.hashtag_counts__squares, 'squares'),
]

Xvals = np.linspace(0, 1, 21) # increments of 0.05

import sys
mn, mx, Y = sys.argv[1:]

for i, X in enumerate(Xvals[int(mn):int(mx)]):
    print i, X
    for region_iter, suffix in region_iters:
        filename = 'unionXY/unionXY_{}_{}_{}.txt'.format(suffix,
                                                         int(X/.05),
                                                         int(Y))
        print filename
        twitterproj.sorted_hashtags_unionXY(X, int(Y), region_iter(), filename)
