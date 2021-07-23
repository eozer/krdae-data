from typing import List

import json
import pandas as pd


def df_to_geojson_and_save(df: pd.DataFrame,
                           properties: List[str] = ["timestamp", "depth_km",
                                                    "MD", "ML", "Mw", "place",
                                                    "location"], lat='latitude',
                           lon='longtitude', filename='tmp.json'):
    # json cannot serialize timestamp types
    df['timestamp'] = df['timestamp'].astype(str)
    geojson = {'type': 'FeatureCollection', 'features': []}
    for _, row in df.iterrows():
        feature = {'type': 'Feature', 'properties': {},
                   'geometry': {'type': 'Point', 'coordinates': []}}
        # according to geojson first number is longtitude.
        feature['geometry']['coordinates'] = [row[lon], row[lat]]
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    with open(filename, 'w') as f:
        json.dump(geojson, f, indent=2)
