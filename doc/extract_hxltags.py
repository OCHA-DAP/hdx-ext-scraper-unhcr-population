import pandas as pd
from pandas_ods_reader import read_ods
import json

df = read_ods("fields.ods", 1)
hxltags={}
for i, row in df.iterrows():
    column = row.Column
    tag = row.Tag
    if column in hxltags:
        if hxltags[column]!=tag:
            print(f"Discrepancy at index {i}, {hxltags[column]} != {tag}")
    else:
        hxltags[column]=tag

with open("hxltags.json", "w") as f:
    json.dump(hxltags,f)