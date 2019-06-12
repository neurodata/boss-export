"""Lambda to export data from BOSS
Entry point is an object inside the boss s3 bucket
Output is a neuroglancer gzip compressed object at the correct path and bucket
"""


import blosc
import boto3


def lambda_handler(event, context):
    s3Client = boto3.client("s3")

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
    # - figure out where it will go (compute neuroglancer path)

    # handle the object
    # - decompress the object from boss format to numpy
    # - compress the object to neuroglancer format (gzip serialized numpy)

    # saving it out
    # - save object to the target bucket and path

    results = [
        {"taskId": taskId, "resultCode": "Succeeded", "resultString": "Succeeded"}
    ]

    return {
        "invocationSchemaVersion": invocationSchemaVersion,
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": invocationId,
        "results": results,
    }


# we need to decode the object (blosc decompress the object) -> numpy array

# serialize the numpy array (F order?)

# gzip compress the object

# extract morton ID, res, and other metadata from obj key

# save the object to the target bucket & path

# set metadata on object (compression -> gzip)
