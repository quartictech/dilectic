from xlrd import open_workbook
wb = open_workbook('/Users/arlogb/quartic/dilectic/data/london_datastore/GVA-GDHI-nuts3-regions-uk.xls')

of_interest = ['GVA', 'GVA Per Head', 'per head Indicies',
                'Headline gross disposible house', 'GDHI per Head']


for s in wb.sheets():
    if s.name in of_interest:
        for row in range(s.nrows):
            values = []
            for col in range(s.ncols):
                values.append(str(s.cell(row,col).value))
            print(','.join(values))
