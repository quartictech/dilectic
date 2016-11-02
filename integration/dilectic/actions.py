import os.path
import shlex
import tempfile
import csv
import logging
import subprocess

def _merge_dicts(a, b):
    d = a.copy()
    d.update(b)
    return d

def mkdir_p(path, **kwargs):
    return _merge_dicts({
        "actions": ["mkdir -p {path}".format(path=path)],
    }, kwargs)

def xls2csv(source, dest, page, **kwargs):
    return _merge_dicts({
        "actions":["""
            xls2csv {source} \
            | awk 'BEGIN {{ RS = ""; FS=""}} {{ print ${page} }}' > {dest}
            """.format(source=source, dest=dest, page=page)],
        "targets": [dest],
        "file_dep": [source],
    }, kwargs)

def xlsx2csv(source, dest, **kwargs):
    return _merge_dicts({
    "name": source,
    "actions": ["xlsx2csv {source} {dest}".format(source=source, dest=dest)],
    "targets": [dest],
    "file_dep": [source]
    }, kwargs)

def shp_to_sql(source, dest, srid, **kwargs):
    table_name = os.path.basename(dest).replace(".sql", "")
    return _merge_dicts({
        "actions": ["""
            shp2pgsql -s {srid} {source} {table_name} > {dest}
            """.format(source=source, dest=dest, srid=srid, table_name=table_name)],
        "file_dep": [source],
    }, kwargs)

def unzip(source, dest, files=[], targets=[], **kwargs):
    return _merge_dicts({
        "actions": [
        """
        unzip -o {source} -d {dest} {files}
        """.format(source=shlex.quote(source), dest=shlex.quote(dest), files=" ".join([shlex.quote(f) for f in files]))],
        "file_dep": [source],
        "targets": [os.path.join(dest, f) for f in files] if files else targets
        }, kwargs)

def db_create(cfg, name, create=None, fill=None, fill_direct=None, sql_file=None, db_config=None, **kwargs):
    def object_exists():
        logging.info("Checking existence of %s", name)
        with cfg.db().cursor() as curs:
            sql = "SELECT to_regclass('{name}')".format(name=name)
            curs.execute(sql)
            return curs.fetchone()[0] is not None

    def object_non_empty():
        with cfg.db().cursor() as curs:
            sql = "SELECT count(*) FROM {name}".format(name=name)
            curs.execute(sql)
            return curs.fetchone()[0] > 0

    def create_object():
        conn = cfg.db()
        with conn.cursor() as curs:
            curs.execute(create)
        conn.commit()

    def fill_table():
        logging.info("creating table %s", name)
        conn = cfg.db()
        with conn.cursor() as curs:
            with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as tmp_file:
                writer = csv.writer(tmp_file, delimiter='\t')
                count = 0
                for row in fill():
                    if count % 10000 == 0:
                        print(count)
                    writer.writerow(row)
                    count += 1
                tmp_file.seek(0)
                with conn.cursor() as curs:
                    curs.copy_from(tmp_file, name, null='')
                conn.commit()

    def fill_sql_file():
        if object_exists():
            conn = cfg.db()
            logging.info("dropping %s", name)
            with conn.cursor() as curs:
                curs.execute("drop table {name} cascade".format(name=name))
            conn.commit()
            assert not object_exists()
        logging.info("creating table %s from sql file: %s", name, sql_file)
        env = os.environ.copy()
        env['PGPASSWORD'] = db_config['password']
        command = "psql -q -h {db[host]} -U {db[user]} {db[dbname]} < {fname}".format(fname=sql_file, db=db_config)
        subprocess.check_call(command, shell=True, env=env)

    def fill_table_direct():
        conn = cfg.db()
        with conn.cursor() as curs:
            fill_direct(curs)
        conn.commit()

    if create:
        yield _merge_dicts({
            "name": "create",
            "actions": [create_object],
            "uptodate": [object_exists]
        }, kwargs)

    if fill:
        yield _merge_dicts({
            "name": "fill",
            "actions": [fill_table],
            "uptodate": [object_non_empty]
        }, kwargs)

    if fill_direct:
        yield _merge_dicts({
            "name": "fill_direct",
            "actions": [fill_table_direct],
            "uptodate": [object_non_empty]
        }, kwargs)

    if sql_file:
        yield _merge_dicts({
            "name": "sql_file",
            "actions": [fill_sql_file],
            "uptodate": [object_exists],
            "file_dep": [sql_file]
        }, kwargs)
