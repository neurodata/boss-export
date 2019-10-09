"""Lambda to export data from BOSS
Consumes messages from an sqs queue
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""

import json

import boto3

from boss_export.libs import bosslib, mortonxyz, ngprecomputed

# will get credentials from role it's running under
S3_RESOURCE = boto3.resource("s3")

BOSS_BUCKET = "cuboids.production.neurodata"
CUBE_SIZE = 512, 512, 16  # boss cube size


def convert_cuboid(msg):
    s3Key = msg["s3key"]
    dtype = msg["dtype"]
    extent = msg["extent"]
    offset = msg["offset"]
    scale = msg["scale"]  # for res 0
    scale_at_res = msg["scale_at_res"]
    res = msg["res"]
    x = msg["x"]
    y = msg["y"]
    z = msg["z"]

    # object naming
    # - decode the object name into its parts: morton ID, res, table keys
    bosskey = bosslib.parts_from_bosskey(s3Key)

    # get obj
    # decompress the object from boss format to numpy
    data_array = bosslib.get_boss_data(
        S3_RESOURCE, BOSS_BUCKET, s3Key, dtype, CUBE_SIZE
    )

    # get the coordinates of the cube
    xyz_coords = mortonxyz.get_coords(bosskey.mortonid, CUBE_SIZE)

    # need to reshape and reset size when at edges
    data_array = ngprecomputed.crop_to_extent(data_array, xyz_coords, extent)

    # boss mortonid has offset embedded in it
    ngmorton = ngprecomputed.ngmorton(bosskey.mortonid, CUBE_SIZE, offset)

    # TODO: handle scale here for res > 0
    # TODO: deal with morton offsets (boss has it, ngprecomputed does not)

    # get the shape of the object
    shape = data_array.shape

    # compute neuroglancer key (w/ offset in name)
    chunk_name = ngprecomputed.get_chunk_name(
        ngmorton, scale, bosskey.res, shape, offset
    )
    ngkey = ngprecomputed.get_ng_key(DEST_DATASET, DEST_LAYER, chunk_name)

    # saving it out
    # compress the object to neuroglancer format (gzip serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array)

    # save object to the target bucket and path
    ngprecomputed.save_obj(S3_RESOURCE, DEST_BUCKET, ngkey, ngdata)


def lambda_handler(event, context):
    for record in event["Records"]:
        msg = json.loads(record["messageAttributes"])
        print(msg)
        convert_cuboid(msg)
