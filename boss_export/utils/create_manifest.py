"""Create a manifest of BOSS objects to use in s3 batch
Inputs: should be name of collection/experiment/channel and resolution(s)
Output: CSV file of each S3 key in the cuboids bucket
"""

import os
import sys
from multiprocessing.pool import Pool
from functools import partial

from boss_export.libs import chunks, mortonxyz, bosslib

# TODO: an interface for running this (e.g. cmdline)
# these are the IDs from the database
coll = 51  # "bock"
exp = 174  # "bock11"
ch = 1005  # "image"

# TODO: get these from BOSS metadata (can use the web API)
x = 135424
y = 119808
z = 4156
OFFSET = 0, 0, 2917
t = 0  # this is always 0

# TODO: this should be specified
res = 0

# this is a constant
CUBE_SIZE = 512, 512, 16


def create_key(xx, yy, zz):
    x_i, y_i, z_i = [i // cubes for i, cubes, o in zip([xx, yy, zz], CUBE_SIZE, OFFSET)]
    mortonid = mortonxyz.XYZMorton(x_i, y_i, z_i)
    # ret_boss_key(col_id, exp_id, chan_id, res, t, mortonid, version=0, parent_iso=None)
    s3key = bosslib.ret_boss_key(coll, exp, ch, res, t, mortonid)
    return s3key


def main():
    open("manifest.csv", "w").close()

    # iterate through the x,y,z
    xx = list(range(OFFSET[0], x, CUBE_SIZE[0]))
    for yy in range(OFFSET[1], y, CUBE_SIZE[1]):
        for zz in range(OFFSET[2], z, CUBE_SIZE[2]):
            create_key_partial = partial(create_key, yy=yy, zz=zz)
            with Pool() as pool:
                keys = pool.map(create_key_partial, xx)
            with open("manifest.csv", "a") as manifest:
                for key in keys:
                    manifest.write(f"{key}\n")


if __name__ == "__main__":
    main()
