"""Lambda to export data from BOSS
Consumes messages from an sqs queue
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""

import json

import boto3
from botocore.exceptions import ClientError

from boss_export.libs import bosslib, mortonxyz, ngprecomputed

# should get credentials from role it's running under
# SESSION = boto3.Session(profile_name="boss-s3")
# S3_RESOURCE = SESSION.resource("s3")
S3_RESOURCE = boto3.resource("s3")


def convert_cuboid(msg):
    print("msg", msg)

    s3Key = msg["s3key"]
    dtype = msg["dtype"]
    # extent = msg["extent"]
    offset = msg["offset"]
    scale = msg["scale"]  # for res 0
    # scale = msg["scale_at_res"]
    extent_at_res = msg["extent_at_res"]
    dest_dataset = msg["layer_path"]
    dest_bucket = msg["dest_bucket"]
    input_cube_size = msg["input_cube_size"]
    chunk_size = msg["chunk_size"]
    compression = msg["compression"]
    boss_bucket = msg["boss_bucket"]

    # object naming
    # - decode the object name into its parts: morton ID, res, table keys
    bosskey = bosslib.parts_from_bosskey(s3Key)

    # get obj
    # decompress the object from boss format to numpy
    try:
        data_array = bosslib.get_boss_data(
            S3_RESOURCE, boss_bucket, s3Key, dtype, input_cube_size
        )
    except ClientError as e:
        print(s3Key, str(e))
        return

    # get the coordinates of the cube
    xyz_coords = mortonxyz.get_coords(bosskey.mortonid, input_cube_size)

    # need to reshape and reset size when at edges
    data_array_crop = ngprecomputed.crop_to_extent(
        data_array, xyz_coords, extent_at_res
    )

    # boss mortonid has offset embedded in it
    ngmorton = ngprecomputed.ngmorton(bosskey.mortonid, input_cube_size, offset)

    # TODO: handle scale here for res > 0
    # TODO: deal with morton offsets (boss has it, ngprecomputed does not)

    # get the shape of the object
    shape = data_array_crop.shape

    # compute neuroglancer key (w/ offset in name)
    chunk_name = ngprecomputed.get_chunk_name(
        ngmorton, scale, bosskey.res, chunk_size, shape, offset
    )
    ngkey = ngprecomputed.get_ng_key(dest_dataset, None, chunk_name)

    # saving it out
    # compress the object to neuroglancer format (gzip serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array_crop, compression)

    # save object to the target bucket and path
    ngprecomputed.save_obj(
        S3_RESOURCE, dest_bucket, ngkey, ngdata, "STANDARD", compression
    )

    print("Converted to:", ngkey)


def lambda_handler(event, context):
    for record in event["Records"]:
        msg = record["body"]
        # coming from SQS, the body is a string
        if type(msg) == str:
            msg = json.loads(msg)
        convert_cuboid(msg)
