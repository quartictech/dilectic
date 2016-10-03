import csv
import os.path

def fill_mcdonalds_table(data_dir):
    path = os.path.join(data_dir, "raw", 'mcdonalds.csv')
    with open(path) as f:
        rdr = csv.reader(f)
        next(rdr)
        for line in rdr:
            if len(line) < 7:
                continue
            values = (line[0], line[1], line[2], line[3], line[4], line[5], line[6])

            yield values
