import os.path
import csv
def fill_borough_profiles(data_dir):
    f = open(os.path.join(data_dir, "london-borough-profiles.csv"), encoding='Windows-1252')
    rdr = csv.reader(f)
    next(rdr)
    for row in rdr:
        yield tuple(row[0:17] + [row[72]] + [row[83]])
