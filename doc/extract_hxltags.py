import pandas as pd
from pandas_ods_reader import read_ods
import json
import yaml

df = read_ods("fields.ods", 1)
hxltags={}
config={}
for i, row in df.iterrows():
    column = row.Column
    tag = row.Tag
    name = row["Alternative column name"]
    if column in hxltags:
        if hxltags[column]!=tag:
            print(f"Discrepancy at index {i}, {hxltags[column]} != {tag}")
    else:
        hxltags[column]=tag
    config[column]=dict(name=name, tags=tag)

with open("hxltags.json", "w") as f:
    json.dump(hxltags,f)
with open("config.yaml", "w") as f:
    f.write(yaml.dump(dict(fields=config), sort_keys=True))
