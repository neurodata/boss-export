#%%
import gzip

import blosc
import numpy as np
from PIL import Image

from boss_export.libs import chunks, mortonxyz, compression, bosslib

#%%
# these are just random xyz values
x = 67712
y = 59904
z = 3993

offset = 0, 0, 2917

# x = 0
# y = 0
# z = 0

# these need to be the IDs from the database
coll = 51  # "bock"
exp = 174  # "bock11"
ch = 1005  # "image"

res = 0
t = 0

cube_size = 512, 512, 16

xyz_cube_idx = [xyz // c for xyz, c in zip((x, y, z), cube_size)]

m_idx = mortonxyz.XYZMorton(*xyz_cube_idx)

#%%
boss_key_name = bosslib.ret_boss_key(coll, exp, ch, res, t, m_idx)

print(boss_key_name)

#%%
with open(
    "test_data/89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0", "rb"
) as f:
    data = blosc.decompress(f.read())


#%%
data_np = np.frombuffer(data, dtype="uint8")
data_array = data_np.reshape(cube_size[::-1])

#%%
image = Image.fromarray(data_array[0, :, :])
image.save("test_data/image.jpg")
image.save("test_data/image.tiff")


#%%
comp_array = gzip.compress(chunks.encode_raw(data_array))

with open("test_data/ng_file", "wb") as f:
    f.write(comp_array)


#%%
# load in actual neuroglancer "chunk" and decompress it to numpy array
# with gzip.open("test_data/0-256_0-234_3685-3941", "r") as f:
#     ng_source_data = f.read()

# when you download the file from the AWS web console it does the gzip decompression for you on the fly...

with open("test_data/0-256_0-256_2917-3173", "rb") as f:
    # with open("test_data/0-512_11264-11776_3557-3621", "rb") as f:
    ng_source_data = f.read()

# content = compression.decompress(ng_source_data, "gzip")

ng_cube_size = [256, 256, 256]
# ng_cube_size = [512, 512, 64]

ng_source_array = chunks.decode_raw(ng_source_data, ng_cube_size, np.uint8)

ng_source_array_reshape = np.transpose(ng_source_array, [2, 1, 0])

for i in range(64):
    image_ng = Image.fromarray(ng_source_array_reshape[i, :, :])
    image_ng.save(f"test_data/ng_images/image_ng_{i}.jpg")

