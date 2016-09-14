from pprint import pprint
from imposm.parser import OSMParser
import os.path
import json

# simple class that handles the parsed OSM data.
class Amenities(object):
    amenities = []

    amenity_types = set()

    def nodes(self, nodes):
        for osmid, tags, loc in nodes:
            if 'amenity' in tags and tags['amenity'] not in self.amenity_types:
                self.amenity_types.add(tags['amenity'])
            if 'amenity' in tags:
                self.amenities.append((osmid, tags['amenity'], tags['name'] if 'name' in tags else None, loc[1],
                                       loc[0], tags['opening_hours'] if 'opening_hours' in tags else None))

def fill_amenities(data_dir):
  amenities = Amenities()
  p = OSMParser(concurrency=4, nodes_callback=amenities.nodes)
  p.parse(os.path.join(data_dir, 'greater-london-latest.osm.pbf'))

  for amenity in amenities.amenities:
      yield amenity

