import csv
import os.path
from datetime import datetime
import json
from collections import defaultdict

def make_ts(values):
    migration_ts = defaultdict(list)
    for k, v in values.items():
        migration_year = k.rsplit("_", 1)
        m_type = migration_year[0]
        year = migration_year[1]
        timestamp = int(datetime(int(year), 1, 1).strftime("%s")) * 1000
        migration_ts[m_type].append({"timestamp": timestamp, "value": int(v.replace(',',''))})
    # print(migration_ts)
    return migration_ts

def fill_migration_borough_table(data_dir,cur):
    path = os.path.join(data_dir, 'derived',
        'net-migration-natural-change-region-borough.csv')

    with open(path) as f:
        rdr = csv.reader(f)
        next(rdr)
        headers = next(rdr)
        #table like code, name, migration_type_ts1, migration_type_ts2
        for line in rdr:
            if len(line) == 0 or line[0] == '':
                continue
            ts = make_ts(dict(zip(headers[2:],line[2:])))
            cur.execute("INSERT INTO migration_boroughs VALUES (%s, %s, %s, %s, %s, %s)",
                        (line[0], line[1], json.dumps({'type' : 'timeseries', 'series' : ts['natchange']}),
                        json.dumps({'type' : 'timeseries', 'series' : ts['international_net']}),
                        json.dumps({'type' : 'timeseries', 'series' : ts['internal_net']}),
                        json.dumps({'type' : 'timeseries', 'series' : ts['other_change']})))


if __name__ == "__main__":
    fill_migration_borough_table('../../data/',None)
