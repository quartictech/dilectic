import os.path
from pprint import pprint
import json

def fill_tube(data_dir):
    with open(os.path.join(data_dir, "raw", "tfl_lines.json")) as f:
        data = json.load(f)
        for row in data["features"]:
            props = row["properties"]
            lines = props["lines"]
            unclosed_lines = [line for line in lines if not "closed" in line]
            if len(unclosed_lines) > 0:
                yield (props["id"], unclosed_lines[0]["name"], unclosed_lines[0]["colour"], row["geometry"])
