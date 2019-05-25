#%%
from gzip import compress

import blosc
import numpy as np
from PIL import Image

from boss_export.boss import boss_key
from boss_export.libs import chunks, mortonxyz

#%%

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
boss_key_name = boss_key.ret_boss_key(coll, exp, ch, res, t, m_idx)

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
comp_array = compress(chunks.encode_raw(data_array))

with open("test_data/ng_file", "wb") as f:
    f.write(comp_array)
