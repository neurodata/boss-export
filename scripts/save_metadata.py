"""get all boss metadata"""

import decimal
import json

import boto3
import pandas as pd


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


SESSION = boto3.Session(profile_name="ben-boss-dev")
dynamodb = SESSION.resource("dynamodb", region_name="us-east-1")

table = dynamodb.Table("bossmeta.production.neurodata")

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
response = table.scan()
items = response["Items"]

for item in items:
    # item["metavalue"] = json.dumps(item["metavalue"])
    lookup_ids = item["lookup_key"].split("&")
    id_cats = "coll", "exp", "ch"
    for id_idx, id in enumerate(lookup_ids):
        item[id_cats[id_idx]] = id

df = pd.DataFrame(items)
df.to_csv("scripts/boss_metadata.csv", index=False)
