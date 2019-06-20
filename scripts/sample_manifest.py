#%%
import random

import boto3
import matplotlib.pyplot as plt
import numpy as np

from boss_export.libs import bosslib, mortonxyz

#%%
with open("manifest.csv", "r") as f:
    lines = f.readlines()

#%%
session = boto3.Session(profile_name="boss-s3")
S3 = session.client("s3")

# this command works...
# aws s3 cp "s3://cuboids.production.neurodata/dac5eaba33ec4c4285c1c9fee822200c&51&174&1005&0&0&9554256&0" . --profile=boss-s3

#%%
n_iter = 1000
n_fails = 0
xy_succ = xy_fail = np.zeros((0, 3))
for n in range(0, n_iter):
    s_line = random.randint(0, len(lines))
    s3key = lines[s_line].strip()
    try:
        bosskey = bosslib.parts_from_bosskey(s3key)
        xyz = mortonxyz.MortonXYZ(bosskey.mortonid)
        # print(morton_id, "xyz", xyz)
        s3_obj = S3.head_object(Bucket="cuboids.production.neurodata", Key=s3key)
        xy_succ = np.append(xy_succ, [xyz], axis=0)
        # print(s3key, "found")

    except Exception:
        # print(s3key, "error", e)
        n_fails += 1
        xy_fail = np.append(xy_fail, [xyz], axis=0)

print(f"missing keys: {n_fails/n_iter*100}%")

#%%
plt.scatter(xy_succ[:, 0], xy_succ[:, 1], c="green")
plt.scatter(xy_fail[:, 0], xy_fail[:, 1], c="red")


#%%
cube = 512, 512, 16
xy_fail_sort = xy_fail[xy_fail[:, 1].argsort()]
for miss in xy_fail_sort:
    print([int(m * c) for m, c in zip(miss, cube)])
