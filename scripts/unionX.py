import twitterproj
import json
import numpy as np


db = twitterproj.connect()
region_iters = [
    #(twitterproj.hashtag_counts__states, 'states'),
    (twitterproj.hashtag_counts__counties, 'counties'),
    #(twitterproj.hashtag_counts__squares, 'squares'),
]

Xvals = np.linspace(0, 1, 21) # increments of 0.05

import sys
mn, mx = sys.argv[1:]

for i, X in enumerate(Xvals[int(mn):int(mx)]):
    print i, X
    for region_iter, suffix in region_iters:
        filename = 'unionX/unionX_{}_{}.txt'.format(suffix, int(X/.05))
        print filename
        twitterproj.sorted_hashtags_unionX(X, region_iter(), filename)
