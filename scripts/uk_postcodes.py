import csv
import os.path

def fill_postcodes_table(conn, data_dir):
    curs = conn.cursor()
    f = open(os.path.join(data_dir, "ukpostcodes.csv"))
    rdr = csv.reader(f)
    next(rdr)
    for row in rdr:
        curs.execute("INSERT INTO uk_postcodes(postcode, geom) VALUES(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))", (row[1], row[3], row[2]))
