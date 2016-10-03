import glob
import csv
import sys
import zipfile
import codecs
import os.path
import utils

def process_companies_csv(f):
    rdr = csv.reader(f)
    next(rdr) # Skip header
    count = 0
    for line in rdr:
        dissolution_date = utils.parse_date(line[13])
        incorporation_date = utils.parse_date(line[14])

        values = (line[0], line[1], line[10], line[11], line[12],
        dissolution_date, incorporation_date, line[26], line[27],
        line[28], line[9])

        yield values

def extract_companies_zip(fname):
    zip = zipfile.ZipFile(fname)
    names = list(zip.namelist())
    print("%s : %d files" % (fname, len(names)))
    for name in names:
        f = codecs.iterdecode(zip.open(name), 'utf-8')
        yield from process_companies_csv(f)

def fill_companies_table(data_dir):
    path = os.path.join(data_dir, "raw", "register/zip/*.zip")
    for f in glob.glob(path):
        yield from extract_companies_zip(f)
