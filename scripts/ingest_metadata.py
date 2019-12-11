#%%
import configparser
import json

import boto3
import pandas as pd
from botocore import exceptions

from boss_export.libs import ngprecomputed

#%%
session = boto3.Session(profile_name="ben-boss-dev")
s3 = session.client("s3")
BUCKET_NAME = "open-neurodata"
PUBLIC_METADATA = "scripts/all_datasets_ids.csv"

config = configparser.ConfigParser()
config.read("scripts/secrets.ini")

iam_role = config["DEFAULT"]["iam_role"]
s3_write_resource = ngprecomputed.assume_role_resource(iam_role, session)

#%%
def check_info(bucket_name, prefix):
    # check if a given prefix has an info file at it's base directory

    prefix = prefix.strip("/")
    key = f"{prefix}/info"

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return 0

    except exceptions.ClientError:
        # Not found
        return 1


def get_subdirs(bucket_name, prefix):
    # gets the subdirectories under a prefix

    resp = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    prefix_names = [r["Prefix"] for r in resp["CommonPrefixes"]]
    return prefix_names


def get_data_dirs(bucket_name, prefix, data_dirs):
    # recursive function to return prefixes with data

    result = check_info(bucket_name, prefix)
    if result != 0:
        subdirs = get_subdirs(bucket_name, prefix)
        for subdir in subdirs:
            data_dirs = get_data_dirs(bucket_name, subdir, data_dirs)
        return data_dirs
    else:
        return data_dirs + [prefix]


#%%
prefixes = get_data_dirs(BUCKET_NAME, "", [])


#%%
# list out prefixes
print(prefixes)

#%%
# load the pandas dataframe w/ all the data
df = pd.read_csv(PUBLIC_METADATA, na_filter=False)

# filter the metadata for only what we need
exclude_metadata = ["to_be_deleted", "deleted_status", "downsample_arn"]
df.drop(exclude_metadata, axis=1, inplace=True)

# %%
for prefix in prefixes:
    parts = prefix.strip("/").split("/")
    key = prefix + "provenance"
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        continue
    except exceptions.ClientError:
        # Not found
        print("sending file...")

    if len(parts) == 3:
        coll, exp, ch = parts

        if exp == "kasthuri14s1colEM" and ch == "anno":
            # renamed this:
            exp = "kasthuri14s1colANNO"
            ch = "annotations"

        row = df[(df["coll"] == coll) & (df["exp"] == exp) & (df["ch"] == ch)]
    elif len(parts) == 2:
        coll = None
        exp, ch = parts
        row = df[(df["exp"] == exp) & (df["ch"] == ch)]
    else:
        raise Exception

    if len(row) != 1:
        print(f"Missing metadata: {prefix}")
        if coll != "templier":
            continue
        else:
            print("using templier provenance")
            provenance = json.dumps(
                {
                    "owners": ["thomas.templier@epfl.ch"],
                    "description": "MagC, magnetic collection of ultrathin sections for volumetric correlative light and electron microscopy",
                    "sources": [],
                    "processing": [],
                    "url": "https://neurodata.io/data/templier2019/",
                }
            )
    else:
        provenance = row.iloc[0].to_json()

    # write the metadata to a provinance.json file at that prefix
    ngprecomputed.save_obj(
        s3_write_resource,
        BUCKET_NAME,
        key,
        provenance,
        storage_class="STANDARD",
        cache_control="no-cache",
        content_type="application/json",
    )
