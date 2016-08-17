import csv
import os.path

def fill_mcdonalds_table(conn, data_dir):
    path = os.path.join(data_dir, 'mcdonalds.csv')
    with open(path) as f:
        rdr = csv.reader(f)
        next(rdr)
        curs = conn.cursor()
        for line in rdr:
            if len(line) < 7:
                continue
            sql = """INSERT INTO mcdonalds VALUES (%s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326))"""
            values = (line[0], line[1], line[2], line[3], line[4], line[5], line[6])

            curs.execute(sql, values)
            conn.commit()
