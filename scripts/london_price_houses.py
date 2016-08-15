from zipfile import ZipFile
import os.path
import csv
import codecs
import utils

def process_prices_zip(conn, path):
    with ZipFile(path) as z:
        for f in z.namelist():
            f = codecs.iterdecode(z.open(f), 'ISO-8859-1')
            rdr = csv.reader(f)
            count = 0
            curs = conn.cursor()
            next(rdr)
            for row in rdr:
                sql = """INSERT INTO london_price_houses VALUES(%s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s
                                                                )"""

                fmt = '%d/%m/%Y %H:%M'
                values = (row[2], utils.parse_date(row[3], fmt), row[8], row[9], row[10],
                row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                row[19], row[20], row[21], row[22], row[23], row[24], row[25])

                curs.execute(sql, values)
                count += 1
                if count % 10000 == 0:
                    print(count)
                    conn.commit()

def fill_london_house_prices(conn, data_dir):
    data = 'london_datastore/London-price-paid-house-price-data-since-1995-CSV.zip'
    path = os.path.join(data_dir, data)
    process_prices_zip(conn, path)
