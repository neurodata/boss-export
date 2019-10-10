"""Lambda to export data from BOSS
Consumes messages from an sqs queue
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""

import json

import boto3

from boss_export.libs import bosslib, mortonxyz, ngprecomputed

# should get credentials from role it's running under
try:
    SESSION = boto3.Session(profile_name="boss-s3")
    S3_RESOURCE = SESSION.resource("s3")
except:
    S3_RESOURCE = boto3.resource("s3")

BOSS_BUCKET = "cuboids.production.neurodata"
CUBE_SIZE = 512, 512, 16  # boss cube size
COMPRESSION = "br"


def convert_cuboid(msg):
    s3Key = msg["s3key"]
    dtype = msg["dtype"]
    extent = msg["extent"]
    offset = msg["offset"]
    scale = msg["scale"]  # for res 0
    # scale = msg["scale_at_res"]
    dest_dataset = msg["layer_path"]
    dest_bucket = msg["dest_bucket"]

    print("Starting", s3Key)

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
        ngmorton, scale, bosskey.res, CUBE_SIZE, shape, offset
    )
    ngkey = ngprecomputed.get_ng_key(dest_dataset, None, chunk_name)

    # saving it out
    # compress the object to neuroglancer format (gzip serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array, COMPRESSION)

    # save object to the target bucket and path
    ngprecomputed.save_obj(
        S3_RESOURCE, dest_bucket, ngkey, ngdata, "STANDARD", COMPRESSION
    )

    print("Converted to:", ngkey)


def lambda_handler(event, context):
    for record in event["Records"]:
        msg = json.loads(record["messageAttributes"])
        convert_cuboid(msg)
