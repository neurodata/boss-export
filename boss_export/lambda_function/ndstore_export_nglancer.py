"""Lambda to export data from ndstore to BOSS
Entry point is an object inside ndstore
Output is a neuroglancer gzip compressed object at the correct path and bucket

# Needs the following access:

s3 read access to ndstore bucket: neurodata-cuboid-store (s3neurodata)
s3 write access to nd-precomputed-volumes (ICC)

"""

import boto3

from boss_export.libs import ndstore, mortonxyz, ngprecomputed

# TODO: dynamically calculate these
DEST_BUCKET = "nd-precomputed-volumes"
DEST_DATASET = "bock11_test"
DEST_LAYER = "image_test"
BASE_SCALE = 4, 4, 40
CUBE_SIZE = 512, 512, 64
dtype = "uint8"
EXTENT = 135424, 119808, 4156  # x, y, z
OFFSET = 0, 0, 2917

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
    ndstorekey = ndstore.parts_from_ndstorekey(s3Key)

    # get obj
    # decompress the object from boss format to numpy
    data_array = ndstore.get_block(s3Key, S3_RESOURCE, s3Bucket)

    # get the coordinates of the cube
    xyz_coords = mortonxyz.get_coords(ndstorekey.mortonid, CUBE_SIZE)

    # need to reshape and reset size when at edges
    data_array = ngprecomputed.crop_to_extent(data_array, xyz_coords, EXTENT)

    # TODO: handle scale here for res > 0
    # TODO: deal with morton offsets (boss has it, ngprecomputed does not)

    # get the shape of the object
    shape = data_array.shape

    # compute neuroglancer key (w/ offset in name)
    chunk_name = ngprecomputed.get_chunk_name(
        ndstorekey.mortonid, BASE_SCALE, ndstorekey.res, CUBE_SIZE, shape, OFFSET
    )
    ngkey = ngprecomputed.get_ng_key(DEST_DATASET, DEST_LAYER, chunk_name)

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
