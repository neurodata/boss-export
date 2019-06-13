"""Create a manifest of BOSS objects to use in s3 batch
Inputs: should be name of collection/experiment/channel and resolution(s)
Output: CSV file of each S3 key in the cuboids bucket
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.path.pardir))

from boss_export.libs import chunks, mortonxyz
from boss_export.boss import boss_key

# TODO: an interface for running this (e.g. cmdline)
# these are the IDs from the database
coll = 51  # "bock"
exp = 174  # "bock11"
ch = 1005  # "image"

# TODO: get these from BOSS metadata (can use the web API)
x = 67712
y = 59904
z = 3993
offset = 0, 0, 2917
t = 0  # this is always 0

# TODO: this should be specified
res = 0

# this is a constant
CUBE_SIZE = 512, 512, 16


def main():
    with open("manifest.csv", "w") as manifest:
        # iterate through the x,y,z
        for xx in range(offset[0], x, CUBE_SIZE[0]):
            for yy in range(offset[1], y, CUBE_SIZE[1]):
                for zz in range(offset[2], z, CUBE_SIZE[2]):
                    x_i, y_i, z_i = [
                        i // cubes
                        for i, cubes, o in zip([xx, yy, zz], CUBE_SIZE, offset)
                    ]
                    mortonid = mortonxyz.XYZMorton(x_i, y_i, z_i)
                    # ret_boss_key(col_id, exp_id, chan_id, res, t, mortonid, version=0, parent_iso=None)
                    s3key = boss_key.ret_boss_key(coll, exp, ch, res, t, mortonid)
                    manifest.write(f"{s3key}\n")


if __name__ == "__main__":
    main()
