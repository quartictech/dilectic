import munger.sources as sources
from munger.catalog import configure
from munger.sources import CSVSource, ZipSource, GlobSource

context = configure("./config.yml")
companies = CSVSource(ZipSource(GlobSource("register/zip/*.zip")))

for row in companies.get(context):
    print(row)
