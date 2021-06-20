#!/usr/bin/env python
"""Fetches entries from http://www.koeri.boun.edu.tr/scripts/lst0.asp and writes pre-processed columns to krdae-data.csv file."""
import re

import pandas as pd
import requests

from bs4 import BeautifulSoup


url = 'http://www.koeri.boun.edu.tr/scripts/lst0.asp'
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, 'html.parser')
pre_text = soup.pre.get_text()
raw_table = pre_text[pre_text.rindex('---')+5:]

pre_df = []
for line in raw_table.splitlines():
    if line:
        pattern = r"^(\S+ \d+:\d+:\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.*)$"
        m = re.match(pattern, line)
        if m:
            timestamp, latitude, longtitude, depth_km, MD, ML, Mw, place_raw_txt = m.groups()
            if MD == '-.-':
                MD = ''
            if Mw == '-.-':
                Mw = ''
            place = ''
            location = ''
            if place_raw_txt:
                m = re.match(r"(.*)(Ýlksel|REVIZE).*", place_raw_txt)
                if m:
                    place = m[1].rstrip()
                    location = place
                    # ONIKI ADALAR (AKDENIZ)
                    # AKDENIZ
                    # MARMARA EREGLISI ACIKLARI-TEKIRDAG (MARMARA DENIZI
                    ps = place.find('(')
                    if ps != -1:
                        pe = place.rfind(')')
                        location = place[ps+1:pe]
            ##
            # Appending to list and converting it to DataFrame is much faster.
            pre_df.append([timestamp, latitude, longtitude,
                           depth_km, MD, ML, Mw, place, location])

df = pd.DataFrame(data=pre_df, columns=['timestamp', 'latitude', 'longtitude',
                                        'depth_km', 'MD', 'ML', 'Mw', 'place',
                                        'location'])
df.to_csv("krdae-data.csv", index=False)
