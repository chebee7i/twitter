"""
Script to write user counts for each region to CSV.

"""

import twitterproj

def main():

    db = twitterproj.connect()
    filenames = ['grids/counties.user_counts.bot_filtered.csv',
                 'grids/states.user_counts.bot_filtered.csv',
                 'grids/squares.user_counts.bot_filtered.csv']

    funcs = ['counties', 'states', 'squares']

    for func, filename in zip(funcs, filenames):
        # The attribute we want is twitterproj.hashtag_counts__{gridtype}
        regions = getattr(twitterproj, 'hashtag_counts__' + func)()
        lines = ["# count"]
        for region in regions:
            lines.append(str(region['user_count']))

        with open(filename, 'w') as f:
            f.write('\n'.join(lines))

if __name__ == '__main__':
    main()
