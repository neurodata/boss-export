# import io

import numpy as np

import boss_export.libs.py_compressed_segmentation as csegpy

# from PIL import Image


# def encode_jpeg(arr):
#     assert arr.dtype == np.uint8

#     # simulate multi-channel array for single channel arrays
#     while arr.ndim < 4:
#         arr = arr[..., np.newaxis]  # add channels to end of x,y,z

#     reshaped = arr.T
#     reshaped = np.moveaxis(reshaped, 0, -1)
#     reshaped = reshaped.reshape(
#         reshaped.shape[0], reshaped.shape[1] * reshaped.shape[2], reshaped.shape[3]
#     )
#     if reshaped.shape[2] == 1:
#         img = Image.fromarray(reshaped[:, :, 0], mode="L")
#     elif reshaped.shape[2] == 3:
#         img = Image.fromarray(reshaped, mode="RGB")
#     else:
#         raise ValueError(
#             "Number of image channels should be 1 or 3. Got: {}".format(arr.shape[3])
#         )

#     f = io.BytesIO()
#     img.save(f, "JPEG")
#     return f.getvalue()


# def decode_jpeg(bytestring, shape, dtype):
#     img = Image.open(io.BytesIO(bytestring))
#     data = np.array(img.getdata(), dtype=dtype)

#     return data.reshape(shape, order="F")


def encode_raw(subvol):
    return subvol.tostring("F")


def decode_raw(bytestring, shape, dtype):
    return np.frombuffer(bytearray(bytestring), dtype=dtype).reshape(shape, order="F")


def encode_compressed_segmentation_pure_python(subvol, block_size):
    return csegpy.encode_chunk(subvol.T, block_size=block_size)
