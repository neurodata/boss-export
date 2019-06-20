"""Lambda to export data from BOSS
Entry point is an object inside the boss s3 bucket
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""

import boto3

from boss_export.libs import bosslib, mortonxyz, ngprecomputed

# TODO: dynamically calculate these
DEST_BUCKET = "nd-precomputed-volumes"
DEST_DATASET = "bock11_test"
DEST_LAYER = "image_test"
BASE_SCALE = [4, 4, 40]
CUBE_SIZE = 512, 512, 16
dtype = "uint8"
EXTENT = 135424, 119808, 4156  # x, y, z
OFFSET = [0, 0, 2917]

# will get credentials from role it's running under
S3_RESOURCE = boto3.resource("s3")


def get_coords_minus_offset(xyz, cube_size, offset):
    xyz_coords = [i * c - o for i, c, o in zip(xyz, cube_size, offset)]
    return xyz_coords


def lambda_handler(event, context):
    # Parse job parameters
    invocationSchemaVersion = event["invocationSchemaVersion"]
    invocationId = event["invocationId"]

    # Process the task
    task = event["tasks"][0]
    taskId = task["taskId"]
    s3Key = task["s3Key"]
    s3BucketArn = task["s3BucketArn"]
    s3Bucket = s3BucketArn.split(":")[-1]
    print("BatchProcessObject(" + s3Bucket + "/" + s3Key + ")")

    # object naming
    # - decode the object name into its parts: morton ID, res, table keys
    bosskey = bosslib.parts_from_bosskey(s3Key)

    # get obj
    # decompress the object from boss format to numpy
    data_array = bosslib.get_boss_data(S3_RESOURCE, s3Bucket, s3Key, dtype, CUBE_SIZE)

    # need to reshape and reset size when at edges
    xyz_index = mortonxyz.MortonXYZ(bosskey.mortonid)  # boss mortonid contains offset

    # TODO: handle scale here for res > 0
    # TODO: deal with morton offsets (boss has it, ngprecomputed does not)
    xyz = get_coords_minus_offset(xyz_index, CUBE_SIZE, OFFSET)

    data_array = ngprecomputed.crop_to_extent(data_array, xyz, EXTENT)

    # get the shape of the object
    shape = data_array.shape

    # compute neuroglancer key
    ngkey_part = ngprecomputed.get_key(
        bosskey.mortonid, BASE_SCALE, bosskey.res, shape, OFFSET
    )
    ngkey = f"{DEST_DATASET}/{DEST_LAYER}/{ngkey_part}"

    # saving it out
    # compress the object to neuroglancer format (gzip serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array)

    # save object to the target bucket and path
    ngprecomputed.save_obj(S3_RESOURCE, DEST_BUCKET, ngkey, ngdata)

    results = [
        {"taskId": taskId, "resultCode": "Succeeded", "resultString": "Succeeded"}
    ]

    return {
        "invocationSchemaVersion": invocationSchemaVersion,
        "treatMissingKeysAs": "PermanentFailure",  # other options are "Successful", "TemporaryFailure"
        "invocationId": invocationId,
        "results": results,
    }
