#%%
import random

import boto3

from boss_export.boss import boss_key
from boss_export.libs import mortonxyz

#%%
with open("manifest.csv", "r") as f:
    lines = f.readlines()

#%%
session = boto3.Session(profile_name="boss-s3")
S3 = session.client("s3")

# this command works...
# aws s3 cp "s3://cuboids.production.neurodata/dac5eaba33ec4c4285c1c9fee822200c&51&174&1005&0&0&9554256&0" . --profile=boss-s3

#%%
n_iter = 100
n_fails = 0
for n in range(0, n_iter):
    s_line = random.randint(0, len(lines))
    s3key = lines[s_line].strip()
    try:
        boss_key_parts = boss_key.parts_from_bosskey(s3key)
        morton_id = boss_key_parts[-2]
        # print(morton_id, "xyz", mortonxyz.MortonXYZ(int(morton_id)))
        s3_obj = S3.head_object(Bucket="cuboids.production.neurodata", Key=s3key)
        # print(s3key, "found")

    except Exception as e:
        # print(s3key, "error", e)
        n_fails += 1
print(f"missing keys: {n_fails/n_iter*100}%")

#%%
