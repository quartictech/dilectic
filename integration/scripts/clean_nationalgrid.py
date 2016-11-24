import json
import os

for f in os.listdir('./'):
    if '.geojson' in f:
        print(f)
        with open(f) as in_file:
            j = json.load(in_file)
            if "crs" in j.keys():
                del j["crs"]
            for feature in j['features']:
                if 'OBJECTID' in feature['properties']:
                    feature['id'] = feature['properties']['OBJECTID']
                else:
                    feature['id'] = feature['properties']['GDO_GID']
                for k, v in feature['properties'].items():
                    if v == None:
                        del feature['properties'][k]
            with open(f.split('.')[0] + '_id.geojson', 'w') as outfile:
                json.dump(j, outfile, indent=1)
