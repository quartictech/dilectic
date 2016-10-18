import csv
import os.path
from datetime import datetime
import zipfile
import io
from collections import defaultdict



def make_ts(year):
    return int(datetime(int(year), 1, 1).strftime("%s")) * 1000

def fill_ni_borough_table(data_dir, cur):
    path = os.path.join(data_dir, 'raw', 'nino-registrations-borough.zip')

    zfile = zipfile.ZipFile(path, 'r')
    borough_dict = defaultdict(list)
    for f in zfile.namelist():
        try:
            year = f.split('-')[-1].rstrip('.csv')
            ex_file = zfile.open(f)
            content = io.TextIOWrapper(io.BytesIO(ex_file.read()))
            reader = csv.DictReader(content)
            print(year)
            for r in reader:
                try:
                    borough_dict[r['Code']].append((year, int(r['France'].replace(',', ''))))
                except Exception as e:
                    pass
        except UnicodeDecodeError as e:
            print('fuck unicode.')
    print('test')
    print(borough_dict)
        # for b in ts.keys():
        #     ("INSERT INTO nino_registration_boroughs VALUES (%s, %s, %s)")
        #     print({'type' : 'timeseries', 'series' : ts[b]})


if __name__ == "__main__":
    fill_ni_borough_table('../../data', None)
