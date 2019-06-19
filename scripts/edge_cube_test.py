#%%
import boto3
from PIL import Image

from boss_export.libs import bosslib, mortonxyz

# * BOSS and ngprecomputed keys start from offset

#%%
# bock11 extents
EXTENT = (135424, 119808, 4156)
OFFSET = 0, 0, 2917
t = 0  # this is always 0
res = 0

# these are the IDs from the database
coll = 51  # "bock"
exp = 174  # "bock11"
ch = 1005  # "image"
dtype = "uint8"

CUBE_SIZE = 512, 512, 16

session = boto3.Session(profile_name="boss-s3")
S3 = session.resource("s3")
BUCKET = "cuboids.production.neurodata"

#%%
# only edge with keys that exist is along z axis and maybe y...

# get coords for edge of z
z_edge = EXTENT[2] // CUBE_SIZE[2] * CUBE_SIZE[2]

x_mid, y_mid = [i // c // 2 * c for i, c in zip(EXTENT[0:2], CUBE_SIZE[0:2])]

print(x_mid, y_mid, z_edge)


#%%
# get a cube from the boss at these edges
mortonid = mortonxyz.XYZMorton(
    *[i // c for i, c in zip([x_mid, y_mid, z_edge], CUBE_SIZE)]
)
s3key = bosslib.ret_boss_key(coll, exp, ch, res, t, mortonid)
print(s3key)

data = bosslib.get_boss_data(S3, BUCKET, s3key, dtype, CUBE_SIZE)
print(data.shape)
# 16, 512, 512
# so... we have full "cubes" of data all the way out to the edges in boss
# but is it appropriately filled?


#%%

for i, zsl in enumerate(data):
    img = Image.fromarray(zsl)
    img.save(f"test_data/{x_mid}_{y_mid}_{i+z_edge}.png")


#%%
