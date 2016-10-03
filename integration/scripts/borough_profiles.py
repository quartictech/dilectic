import os.path
import csv
def fill_borough_profiles(data_dir):
    f = open(os.path.join(data_dir, "raw", "london-borough-profiles.csv"), encoding='Windows-1252')
    rdr = csv.reader(f)
    next(rdr)
    for row in rdr:
        v= tuple(row[1:17] + [row[38], row[41], row[45], row[48]] + [row[74]] + row[79:])
        yield v
