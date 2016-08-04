import csv

def fill_postcodes_table(conn):
    curs = conn.cursor()
    f = open("../data/ukpostcodes.csv")
    rdr = csv.reader(f)
    next(rdr)
    for row in rdr:
        curs.execute("INSERT INTO uk_postcodes(postcode, geom) VALUES(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))", (row[1], row[3], row[2]))
