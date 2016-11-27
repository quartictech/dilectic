
from dilectic.utils import *
import unittest
import os.path

config_file = os.path.join(os.path.dirname(__file__), 'config.yml')
cfg = Config(config_file)

class TestDataIntegrations(unittest.TestCase):
    def table_exists(self, name):
        with cfg.db().cursor() as curs:
            sql = "SELECT to_regclass('{name}')".format(name=name)
            curs.execute(sql)
            return curs.fetchone()[0] is not None

    def table_non_empty(self, name):
        with cfg.db().cursor() as curs:
            sql = "SELECT count(*) FROM {name}".format(name=name)
            curs.execute(sql)
            return curs.fetchone()[0] > 0

    def assertTable(self, name):
        self.assertTrue(self.table_exists(name), "table {0} does not exist".format(name))
        self.assertTrue(self.table_non_empty(name), "table {0} is empty".format(name))

    def assertFile(self, path):
        return self.assertTrue(os.path.exists(path), "file {0} does not exist".format(path))

    def final_file(self, rel):
        return os.path.join(cfg.final_dir, rel)

    def test_billboards(self):
        self.assertFile(self.final_file("signkick.geojson"))

    def test_disruptions(self):
        self.assertFile(self.final_file("disruptions.geojson"))

    def test_osm(self):
        self.assertTable("planet_osm_roads")
        self.assertTable("planet_osm_polygon")
        self.assertTable("planet_osm_line")
        self.assertTable("planet_osm_point")

    def test_boroughs(self):
        self.assertTable("london_borough_profiles")
        self.assertTable("london_borough_excluding_mhw")

    def test_crime(self):
        self.assertTable("crime")
        self.assertTable("crime_geocoded")

    def test_jamcams(self):
        self.assertTable("jamcams")
        self.assertTable("jamcams_geocoded")

    def test_london_price_houses(self):
        self.assertTable("london_price_houses")
        self.assertTable("london_price_houses_geocoded")

    def test_uk_postcodes(self):
        self.assertTable("uk_postcodes")

    def test_public_land_assets(self):
        self.assertTable("public_land_assets_geocoded")

if __name__ == '__main__':
    unittest.main()
