import csv
import os.path

def fill_postcodes_table(data_dir):
    f = open(os.path.join(data_dir, "derived", "ukpostcodes.csv"))
    rdr = csv.reader(f)
    next(rdr)
    for row in rdr:
        yield (row[1], row[2], row[3])
