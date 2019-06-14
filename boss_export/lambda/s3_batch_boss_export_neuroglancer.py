"""Lambda to export data from BOSS
Entry point is an object inside the boss s3 bucket
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""

import blosc
import boto3

from boss_export.libs import mortonxyz, ngprecomputed, bosslib

# TODO: dynamically calculate these
DEST_BUCKET = "nd-precomputed-volumes"
DEST_DATASET = "bock11"
DEST_LAYER = "image"
BASE_SCALE = [4, 4, 40]
CUBE_SIZE = 512, 512, 16
dtype = "uint8"

# will get credentials from role it's running under
S3_RESOURCE = boto3.resource("s3")


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
    parent_iso, col_id, exp_id, chan_id, res, t, mortonid, version = bosslib.parts_from_bosskey(
        s3Key
    )

    # get obj
    # decompress the object from boss format to numpy
    data_array = bosslib.get_boss_data(S3_RESOURCE, s3Bucket, s3Key, dtype, CUBE_SIZE)

    # get the shape of the object
    shape = data_array.shape

    # compute neuroglancer key
    ngkey_part = ngprecomputed.get_key(mortonid, BASE_SCALE, res, shape)
    ngkey = f"{DEST_DATASET}/{DEST_LAYER}/{ngkey_part}"

    # saving it out
    # compress the object to neuroglancer format (gzip serialized numpy)
    ngdata = ngprecomputed.numpy_chunk(data_array)

    # save object to the target bucket and path
    S3_RESOURCE

    # set metadata on object (compression -> gzip)

    results = [
        {"taskId": taskId, "resultCode": "Succeeded", "resultString": "Succeeded"}
    ]

    return {
        "invocationSchemaVersion": invocationSchemaVersion,
        "treatMissingKeysAs": "PermanentFailure",  # other options are "Successful", "TemporaryFailure"
        "invocationId": invocationId,
        "results": results,
    }
