"""Lambda to export data from BOSS
Consumes messages from an SQS queue
Output is a neuroglancer compressed object at the correct path and bucket
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

    if "iam_role" in msg:
        s3_write_resource = ngprecomputed.assume_role_resource(msg["iam_role"], boto3)
    else:
        s3_write_resource = S3_RESOURCE

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
    public = msg["public"]
    owner_id = msg["owner_id"]

    if msg["hierarchy_method"] == "isotropic":
        iso = True
    else:
        iso = False

    print("getting bosskey parts")

    # object naming
    # - decode the object name into its parts: morton ID, res, table keys
    bosskey = bosslib.parts_from_bosskey(s3Key)

    print("getting data from boss")

    # get obj
    # decompress the object from boss format to numpy
    try:
        data_array = bosslib.get_boss_data(
            S3_RESOURCE, boss_bucket, s3Key, dtype, input_cube_size
        )
    except ClientError as e:
        print(s3Key, str(e))
        return

    print("getting xyz from morton")

    # get the coordinates of the cube
    xyz_coords = mortonxyz.get_coords(bosskey.mortonid, input_cube_size)

    print("cropping data to extent")

    # need to reshape and reset size when at edges
    data_array_crop = ngprecomputed.crop_to_extent(
        data_array, xyz_coords, extent_at_res
    )

    print("getting ng morton id")

    # boss mortonid has offset embedded in it
    ngmorton = ngprecomputed.ngmorton(bosskey.mortonid, input_cube_size, offset)

    # get the shape of the object
    shape = data_array_crop.shape

    print("compute ng s3key")

    # compute neuroglancer key (w/ offset in name)
    chunk_name = ngprecomputed.get_chunk_name(
        ngmorton, scale, bosskey.res, chunk_size, shape, offset, iso=iso
    )
    ngkey = ngprecomputed.get_ng_key(dest_dataset, None, chunk_name)

    print("compressing the array")

    # compress the object to neuroglancer format (compressed serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array_crop, compression)

    print("saving the ng object")

    # save object to the target bucket and path
    ngprecomputed.save_obj(
        s3_write_resource,
        dest_bucket,
        ngkey,
        ngdata,
        storage_class="INTELLIGENT_TIERING",
        content_encoding=compression,
        public=public,
        owner_id=owner_id,
    )

    print("Converted to:", ngkey)


def lambda_handler(event, context):
    for record in event["Records"]:
        msg = record["body"]
        # coming from SQS, the body is a string
        if type(msg) == str:
            msg = json.loads(msg)
        convert_cuboid(msg)
