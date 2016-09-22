import csv

def fill_naptan(data_dir):
    path = os.path.join(data_dir, 'NaPTANcsv')
    with  open(path) as f:
        rdr = csv.reader(f)
        next(rdr)
        for line in rdr:
            yield line
