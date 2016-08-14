from xlrd import open_workbook
wb = open_workbook('/Users/arlogb/quartic/dilectic/data/london_datastore/GVA-GDHI-nuts3-regions-uk.xls')

of_interest = ['GVA', 'GVA Per Head', 'per head Indicies',
                'Headline gross disposible house', 'GDHI per Head']

def process_gva_sheet(s):
    years = s.row(0)[3:]
    print((years[0].value))
    print(len(years))
    for row in range(s.nrows):
        if str(s.cell(row,1).value)[:3] == "UKI":
            for n, v in enumerate(years):
                insert = []
                insert.append(str(s.cell(row,1).value))
                insert.append(int(v.value))
                insert.append(str(s.cell(row,2).value))
                insert.append(int(s.cell(row,n+3).value))
                print(','.join(str(v) for v in insert))

for s in wb.sheets():
    if s.name == 'GVA':
        process_gva_sheet(s)
