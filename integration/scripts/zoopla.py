import requests
import sys
import json
from xml.etree import ElementTree

API="http://api.zoopla.co.uk/api/v1/property_listings.xml"
API_KEY="cbf7xffep33z23v7rpyxr5hc"

if __name__ == "__main__":
    out_path = sys.argv[1]
    page = 0
    features = []
    while True:
        print(page)
        r = requests.get(API, params={"lat_min": 51.28, "lat_max": 51.686,
        "lon_min": -0.489, "lon_max": 0.236, "api_key": API_KEY, "page_number": page, "page_size": 100})
        print(r.content)
        tree = ElementTree.fromstring(r.content)
        listings = list(tree.iter("listing"))
        print(listings)
        if len(listings) == 0:
            break
        for listing in listings:
            attributes = {}
            for child in listing.getchildren():
                attributes[child.tag] = child.text
            print(attributes)
            latitude = attributes["latitude"]
            longitude = attributes["longitude"]
            clean_attributes = {}
            for attribute, value in attributes.items():
                if not value or attribute == "latitude" or attribute == "longitude":
                    continue
                key = attribute
                if attribute == "street_name":
                    key = "Street Name"
                if attribute == "price":
                    key = "Price"
                if attribute == "description":
                    key = "Description"
                if attribute == "agent_name":
                    key = "Agent Name"
                clean_attributes[key] = value
            features.append({
                "type": "Feature",
                "id": attributes["listing_id"],
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "properties" : clean_attributes
            })
        page += 1
    output = {
        "name": "Zoopla Properties London",
        "description": "Zoopla Property Listings for London",
        "icon": "home",
        "attribution": "Data provided by Zoopla",
        "data": {
            "type": "FeatureCollection",
            "features": features
        }
    }
    json.dump(output, open(out_path, "w"), indent=1)
