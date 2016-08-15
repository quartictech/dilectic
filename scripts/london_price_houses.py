from zipfile import ZipFile
import os.path
import csv
import codecs
import utils

def process_prices_zip(conn, path):
    with ZipFile(path) as z:
        for f in z.namelist():
            print(f)
            f = codecs.iterdecode(z.open('London Year_1995-2000.csv'), 'utf-8', errors='ignore')
            rdr = csv.reader(f)
            count = 0
            curs = conn.cursor()
            next(rdr)
            for row in rdr:
                print(row)
                import sys
                sys.exit()
                sql = """INSERT INTO london_price_houses VALUES(%s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s
                                                                )"""

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
                    values = (row[2], utils.parse_date(row[3], fmt), row[8], row[9], row[10],
                    row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                    row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26],
                    row[27], row[28])
                except UnicodeDecodeError as e:
                    print(e)
                    print(row)

                except ValueError as e:
                    try:#retarded change in the input data format..
                        #format 2: ['{0FF377DF-DC87-4D54-90DC-5E200B155704}', '215000', '01-Jan-13', 'HA0 4EE', 'HA04EE', 'F', 'N', 'L', ' ', ' ', 'WEMBLEY', 'BRENT', 'GREATER LONDON', 'A', '2013', '1', '1', '2', '00AEGJ', 'E00002335', 'E01000474', 'E02000118', 'Outer', '2013/1', 'HA04', 'HA0', 'E05000085', 'E05000085', 'E09000005', 'Brent']
                        fmt = '%d-%b-%Y'
                        values = (row[1], utils.parse_date(row[2], fmt), row[3], row[5],
                        row[6], row[7], row[8], row[9], row[10], row[11], row[13], )
                    except:
                        print('yea')

                # curs.execute(sql, values)
                # count += 1
                # if count % 10000 == 0:
                #     print(count)
                #     conn.commit()

def fill_london_house_prices(conn, data_dir):
    data = 'london_datastore/London-price-paid-house-price-data-since-1995-CSV.zip'
    path = os.path.join(data_dir, data)
    process_prices_zip(conn, path)
