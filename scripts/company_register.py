import luigi
import luigi.postgres
import glob
import csv
import sys
import psycopg2
from datetime import datetime
import zipfile
import codecs

class ImportCompanyRegister(luigi.Task):
  def requires(self):
    for f in glob.glob("../data/register/zip/*.zip"):
        yield CompanyRegisterToDatabase(fname=f)

class CompanyRegisterToDatabase(luigi.postgres.CopyToTable):
  fname = luigi.Parameter()
  host="localhost"
  database="postgres"
  user="postgres"
  password="dilectic"
  table="companies"

  columns = [
    ("CompanyName", "VARCHAR"),
    ("CompanyNumber", "VARCHAR UNIQUE"),
    ("CompanyCategory", "VARCHAR"),
    ("CompanyStatus", "VARCHAR"),
	("CountryOfOrigin", "VARCHAR"),
	("DissolutionDate", "DATE"),
	("IncorporationDate", "DATE"),
	("SICText1", "VARCHAR"),
	("SICText2", "VARCHAR"),
	("SICText3", "VARCHAR"),
	("Postcode", "VARCHAR")
  ]

  def _parse_date(self, s):
     if not s:
       return None
     try:
       return datetime.strptime(s, '%d/%m/%Y')
     except:
       raise ValueError("Can't parse " + s)

  def _process_csv(self, f):
    rdr = csv.reader(f)
    next(rdr) # Skip header
    count = 0
    for line in rdr:
        dissolution_date = self._parse_date(line[13])
        incorporation_date = self._parse_date(line[14])

        yield (line[0], line[1], line[10], line[11], line[12],
        dissolution_date, incorporation_date, line[26], line[27],
        line[28], line[9])
        count += 1
        if count % 10000 == 0:
            print(count)

  def rows(self):
    zip = zipfile.ZipFile(self.fname)
    names = list(zip.namelist())
    print("%s : %d files" % (self.fname, len(names)))
    for name in names:
      f = codecs.iterdecode(zip.open(name), 'utf-8')
      yield from self._process_csv(f)
