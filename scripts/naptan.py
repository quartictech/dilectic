import csv
import os
import codecs

def mycsv_reader(csv_reader):
  while True:
    try:
      yield next(csv_reader)
    except csv.Error:
      # error handling what you want.
      pass
    continue
  return

def fill_naptan(data_dir):
    path = os.path.join(data_dir, 'StopAreas.csv')
    with  codecs.open(path) as f:
        rdr = mycsv_reader(csv.reader(f))
        next(rdr)
        try:#some null byte error. Fuck that
            for line in rdr:
                yield line
        except csv.Error:
            pass
