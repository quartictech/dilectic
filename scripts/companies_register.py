import glob
import csv
import sys
import zipfile
import codecs
import os.path
import utils

def process_companies_csv(conn, f):
    rdr = csv.reader(f)
    next(rdr) # Skip header
    count = 0
    curs = conn.cursor()
    for line in rdr:
        dissolution_date = utils.parse_date(line[13])
        incorporation_date = utils.parse_date(line[14])

        sql = "INSERT INTO companies VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (line[0], line[1], line[10], line[11], line[12],
        dissolution_date, incorporation_date, line[26], line[27],
        line[28], line[9])

        curs.execute(sql, values)
        count += 1
        if count % 10000 == 0:
            print(count)
            conn.commit()

def extract_companies_zip(fname, conn):
    zip = zipfile.ZipFile(fname)
    names = list(zip.namelist())
    print("%s : %d files" % (fname, len(names)))
    for name in names:
        f = codecs.iterdecode(zip.open(name), 'utf-8')
        process_companies_csv(conn, f)

def fill_companies_table(conn, data_dir):
    path = os.path.join(data_dir, 'register/zip/*.zip')
    for f in glob.glob(path):
        extract_companies_zip(f, conn)