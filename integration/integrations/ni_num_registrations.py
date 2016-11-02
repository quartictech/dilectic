import csv
import os.path
from datetime import datetime
import zipfile
import io
from collections import defaultdict
import json
import yaml

from dilectic.utils import *
from dilectic.actions import *

def make_ts(values):
    output = {"type": "timeseries", "series": []}
    for year, value in values:
        output["series"].append({
            "timestamp": int(datetime(int(year), 1, 1).strftime("%s")) * 1000,
            "value": value
            })
    return output

def extract_data(data_dir):
    path = os.path.join(data_dir, 'nino-registrations-borough.zip')
    zfile = zipfile.ZipFile(path, 'r')
    country_dict = defaultdict(lambda: defaultdict(list))
    fields = ['Area', 'Code', 'Area Code','Area name']
    for f in zfile.namelist():
        try:
            year = f.split('-')[-1].rstrip('.csv')
            ex_file = zfile.open(f)
            content = io.TextIOWrapper(io.BytesIO(ex_file.read()))
            reader = csv.DictReader(content)
            print(year)
            for r in reader:
                area = r['Area'] if 'Area' in r else r['Area name']

                try:
                    for country in [k for k in r.keys() if not k in fields]:
                        key = country.strip()
                        area = r['Area'].strip()
                        if not key or not area:
                            continue
                        if country in['All registrations', 'Total', 'Total Number of Registrations', 'Overseas Registration']:
                            key = 'Total'
                        country_dict[area][key].append((year, int(r[country].replace(',', ''))))
                except Exception as e:
                    pass
        except UnicodeDecodeError as e:
            print('fuck unicode.')
    return country_dict

def get_table_columns(data_dir):
    country_dict = extract_data(data_dir)
    all_countries = set()
    for borough, countries in country_dict.items():
        all_countries |= countries.keys()
    all_countries = list(all_countries)
    return ",".join(["borough VARCHAR"] + ["%s JSON" % json.dumps(column) for column in
        all_countries])

@task
def ni_num_registrations(cfg):
    def fill_ni_borough_table(cur):
        country_dict = extract_data(cfg.raw_dir)
        all_countries = set()
        for borough, countries in country_dict.items():
            all_countries |= countries.keys()
        all_countries = list(all_countries)
        row_headings = [json.dumps(k) for k in ["borough"] + all_countries]
        for borough, countries in country_dict.items():
            row = [borough]
            for country in all_countries:
                row.append(json.dumps(make_ts(countries[country])))
            values_str = "({0})".format(",".join(["%s"] * len(row)))
            columns_str = "({0})".format(",".join(row_headings))
            cur.execute("INSERT INTO nino_registration_boroughs " + columns_str + " VALUES " + values_str, tuple(row))

    return db_create(cfg, 'nino_registration_boroughs',
    create="""CREATE TABLE IF NOT EXISTS nino_registration_boroughs ({0})""".format(get_table_columns(cfg.raw_dir)),
    fill_direct=fill_ni_borough_table)
