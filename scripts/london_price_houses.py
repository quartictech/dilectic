from zipfile import ZipFile, BadZipFile
import os.path
import csv
import codecs
import utils
import subprocess

def fix_zip_file(path):
    #zip file they provide is fucked so we fix it.
    z = ZipFile(path)
    if z.testzip() != None:
        subprocess.call(["zip", "-FF", path, "--out", path + '.fix'])
        path = path + '.fix'
    return path


def process_prices_zip(conn, path):
    path = fix_zip_file(path)
    with ZipFile(path) as z:
        for f in z.namelist():
            print('Processing {}'.format(f))
            f = codecs.iterdecode(z.open(f), 'utf-8', errors='ignore')
            rdr = csv.reader(f)
            count = 0
            curs = conn.cursor()
            next(rdr)
            for row in rdr:
                sql = """INSERT INTO london_price_houses VALUES(%s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s
                                                                )"""
                if len(row) == 29:
                    try:
                        #format 1:
                        #['861696', '{5C1CC576-24F6-453B-9984-3898CC3AA528}',
                        #'155000', '11/01/1995 00:00',
                        #'1', '1',
                        #'1995', '1995/1',
                        #'N20 9AQ', 'F',
                        #'Y', 'L',
                        #'GREENLEAF COURT, 17',
                        #13 below:
                        # 'FLAT 1', 'OAKLEIGH PARK NORTH',
                        #'LONDON', 'LONDON',
                        #'BARNET', 'GREATER LONDON',
                        #'A', 'N20 9AQ',
                        #'Outer', 'E09000003',
                        #'Barnet', 'E05000058',
                        #'Oakleigh', 'E02000031',
                        #'E01000273', 'E00001314']
                        fmt = '%d/%m/%Y %H:%M'
                        values = (
                        row[2], utils.parse_date(row[3], fmt), row[8], row[9], row[10],
                        row[11], row[12], row[13], row[14], row[15],
                        row[16], row[17], row[18], row[19])
                    except UnicodeDecodeError as e:
                        print(e)
                        print(row)

                elif len(row) == 30:
                    try:#retarded change in the input data format..
                        #format 2: ['{0FF377DF-DC87-4D54-90DC-5E200B155704}', '215000',
                        #'01-Jan-13', 'HA0 4EE',
                        #'HA04EE', 'F',
                        #'N', 'L',
                        #' ', ' ', 'WEMBLEY', 'BRENT', 'GREATER LONDON',
                        #'A', '2013', '1', '1', '2',
                        #'00AEGJ', 'E00002335', 'E01000474', 'E02000118',
                        #'Outer', '2013/1', 'HA04', 'HA0',
                        #'E05000085', 'E05000085', 'E09000005', 'Brent']
                        fmt = '%d-%b-%y'
                        values = (
                        row[1], utils.parse_date(row[2], fmt), row[3], row[5], row[6],
                        row[7], "", "", row[8], row[9],#addr is 2 lines shorter - insert blank
                        row[10], row[11], row[12], row[13])
                    except Exception as e:
                        print(e)

                curs.execute(sql, values)
                count += 1
                if count % 10000 == 0:
                    print(count)
                    conn.commit()

def fill_london_house_prices(conn, data_dir):
    data = 'london_datastore/London-price-paid-house-price-data-since-1995-CSV.zip'
    path = os.path.join(data_dir, data)
    process_prices_zip(conn, path)
