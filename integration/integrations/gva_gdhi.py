from xlrd import open_workbook
import os.path
from dilectic.utils import *
from dilectic.actions import *

def process_gva_sheet(s):
    years = [int(v.value) for v in s.row(0)[3:]]
    if years[-1] == 20143:
        years[-1] = 2014
    insert = []
    for row in range(s.nrows):
        if str(s.cell(row,1).value)[:3] == "UKI":
            for n, v in enumerate(years):
                r = []
                r.append(str(s.cell(row,1).value))
                r.append(v)
                r.append(str(s.cell(row,2).value))
                r.append(int(s.cell(row,n+3).value))
                insert.append(r)
    return insert

def process_snowflakes(s):
    years = [int(v.value) for v in s.row(2)[2:]]
    if years[-1] == 20143:
        years[-1] = 2014
    insert = []
    for row in range(s.nrows):
        if str(s.cell(row,0).value)[:3] == "UKI":
            for n, v in enumerate(years):
                r = []
                r.append(s.cell(row,0).value) #code
                r.append(v) #date
                r.append(s.cell(row,1).value) #place
                if s.cell(row,n+2).value == '':
                    continue
                else:
                    r.append(int(s.cell(row,n+2).value))
                insert.append(r)
    return insert

def collector(insert, d=None):
    if d is None:
        d = {}
    else:
        for i in insert:
            key = ''.join((i[0], str(i[1])))
            if key in d.keys():
                d[key].append(i[-1])
            else:
                assert len(i) == 4
                d[key] = i
    return d

def process_workbook(wb):
    of_interest = ['GVA', 'GVA Per Head', 'per head Indices']
    special_snowflakes = ['Headline gross disposable house', 'GDHI per Head']
    headers = ['UKI', 'Year', 'Area']
    table = {}
    for s in wb.sheets():
        if s.name in of_interest:
            headers.append(s.name)
            table = collector(process_gva_sheet(s), table)
        if s.name in special_snowflakes:
            headers.append(s.name)
            table = collector(process_snowflakes(s), table)
    table['headers'] = headers
    return table

@task
def london_gva_gdhi(cfg):
    def gva_gdhi_to_pg():
        wb = open_workbook(os.path.join(cfg.raw_dir, 'GVA-GDHI-nuts3-regions-uk.xls'))
        table = process_workbook(wb)
        table.pop('headers')
        for k, v in table.items():
            #gross hack for data we don't like
            if len(v) != 8:
                continue
            else:
                yield v
    return db_create(cfg, 'london_gva_gdhi',
        create="""CREATE TABLE IF NOT EXISTS london_gva_gdhi (
            UKI VARCHAR,
            Year INT,
            AreaName VARCHAR,
            GVA INT,
            GVAPerHead INT,
            PerHeadIndices INT,
            GrossDisposableHouse INT,
            GDHIPerHead INT)
        """,
        fill=gva_gdhi_to_pg)
