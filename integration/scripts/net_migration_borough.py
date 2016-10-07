import csv
import os.path
from datetime import datetime
import json

def make_ts(values):
    migration_ts = {}
    for k, v in values.items():
        migration_year = k.rsplit("_", 1)
        m_type = migration_year[0]
        year = migration_year[1]
        if m_type in migration_ts.keys():
            migration_ts[m_type]["{}-{}-{}".format(int(year), 1, 1)] = int(v.replace(',',''))
        else:
            migration_ts[m_type] = {"{}-{}-{}".format(int(year), 1, 1):int(v.replace(',',''))}
    # print(migration_ts)
    return migration_ts

def fill_migration_borough_table(data_dir,cur):
    path = os.path.join(data_dir, 'derived',
        'london_datastore/net-migration-natural-change-region-borough',
        'Net Change Borough-Table 1.csv')

    with open(path) as f:
        rdr = csv.reader(f)
        next(rdr)
        headers = next(rdr)
        #table like code, name, migration_type_ts1, migration_type_ts2
        for line in rdr:
            if line[0] == '':
                continue
            ts = make_ts(dict(zip(headers[2:],line[2:])))
            cur.execute("INSERT INTO migration_boroughs VALUES (%s, %s, %s, %s, %s, %s)",
                        (line[0], line[1], json.dumps(ts['natchange']), json.dumps(ts['international_net']),
                        json.dumps(ts['internal_net']), json.dumps(ts['other_change'])))


if __name__ == "__main__":
    fill_migration_borough_table('../../data/')
