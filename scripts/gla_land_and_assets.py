import os
import csv
import glob

data_dir = '/Users/arlogb/quartic/dilectic/data/'

def process_csv(conn, f):
    owner = f.split('/')[-1].strip('_assets.csv')
    with open(f) as csvfile:
        rdr = csv.reader(csvfile)
        next(rdr) #jump the headers
        curs = conn.cursor()
        for line in rdr:
            for index,element in enumerate(line):
                if element.strip() == '':
                    line[index] = None

            line.insert(0, owner)
            yield line

def fill_land_and_assets_table(data_dir):
    path = os.path.join(data_dir, 'london_datastore/gla_land_assets/*.csv')
    for f in glob.glob(path):
        yield from process_csv(f)

if __name__ == "__main__":
    fill_land_and_assets_table(None, data_dir)
