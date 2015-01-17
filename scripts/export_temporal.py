"""

Exports the data in db.grids.temporal.counties to CSV.

"""

import twitterproj

db = twitterproj.connect()

header = \
"""# COUNTS are tweet counts in 5 minute intervals: 24 * 60 / 5 columns
# weekday is the day of week, Mon=0, ..., Sun=6
# state_fips, county_fips, offset_to_eastern, year, month, day, weekday, COUNTS
"""

offset_to_eastern = {'Eastern': 0, 'Central': -1, 'Mountain': -2, 'Pacific': -3}

rows = []
cols = ['state_fips', 'county_fips', 'year', 'month', 'day', 'weekday']
for doc in db.grids.temporal.counties.find():
	row = []
	for col in cols:
		if col == 'date':
			row.append(doc[col].isoformat())
		else:
			row.append(doc[col])

	date = doc['date']
	tz = twitterproj.get_ustz(date, doc['timezone'])
	offset = offset_to_eastern[tz]
	row.insert(2, offset)
	row.extend(doc['counts'])

	row = map(str, row)
	rows.append(','.join(row))

out = header
out += '\n'.join(rows)

with open('temporal.csv', 'w') as fobj:
	fobj.write(out)
