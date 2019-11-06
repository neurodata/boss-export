"""given a channel/experiment/collection
outputs messages in SQS for every cuboid in BOSS
"""

import itertools
import json
import os
from functools import partial

import boto3
import click
import pandas as pd
from botocore.exceptions import ClientError

from boss_export.libs import bosslib, mortonxyz, ngprecomputed
from cloudvolume import CloudVolume

# from multiprocessing import Pool


SESSION = boto3.Session(profile_name="ben-boss-dev")
S3_RESOURCE = SESSION.resource("s3")

# if we set env. variables,
# and it doesn't find the secret in ~/.cloudvolume
# then cloudvolume uses them
credentials = SESSION.get_credentials()
os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key


SQS = SESSION.resource("sqs", region_name="us-east-1")
SQS_NAME = "copy-boss-cuboids"

# globals
PUBLIC_METADATA = "scripts/public_datasets_downsample.csv"

BOSS_BUCKET = "cuboids.production.neurodata"
T = 0  # this is always 0
CUBE_SIZE = 512, 512, 16  # constant for BOSS

COMPRESSION = "br"


def create_precomputed_volume(s3_resource, **kwargs):
    """Use CloudVolume to create the precomputed info file"""

    vol_size = [e - o for e, o in zip(kwargs["extent"], kwargs["offset"])]
    max_mip = (
        kwargs["num_hierarchy_levels"] - 1
        if kwargs["downsample_status"] == "DOWNSAMPLED"
        else 1
    )
    factor = (2, 2, 1) if kwargs["hierarchy_method"] == "anisotropic" else (2, 2, 2)

    info = CloudVolume.create_new_info(
        num_channels=1,
        layer_type=kwargs["layer_type"],
        data_type=kwargs["dtype"],  # Channel images might be 'uint8'
        encoding=kwargs[
            "encoding"
        ],  # raw, jpeg, compressed_segmentation, fpzip, kempressed
        resolution=kwargs["scale"],  # Voxel scaling, units are in nanometers
        voxel_offset=kwargs["offset"],  # x,y,z offset in voxels from the origin
        # Pick a convenient size for your underlying chunk representation
        # Powers of two are recommended, doesn't need to cover image exactly
        chunk_size=kwargs["chunk_size"],  # units are voxels
        volume_size=vol_size,  # units are voxels
        # undocumented param that creates the info w/ that many scales
        max_mip=max_mip,
        factor=factor,
    )

    # Don't use cloudvolume to submit the JSON, just use boto3 directly
    layer_path = kwargs["layer_path"]
    infokey = f"{layer_path}/info"
    resp = ngprecomputed.save_obj(
        s3_resource,
        kwargs["dest_bucket"],
        infokey,
        json.dumps(info),
        content_encoding="",
        cache_control="no-cache",
        content_type="application/json",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def create_or_get_queue():
    try:
        queue = SQS.get_queue_by_name(QueueName=SQS_NAME)
    except ClientError as e:
        print(str(e))
        # create the queue
        queue = SQS.create_queue(
            QueueName=SQS_NAME,
            Attributes={
                "DelaySeconds": "5",
                "ReceiveMessageWaitTimeSeconds": "5",
                "VisibilityTimeout": "600",  # lamda timeout is 1.5 minutes
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": "arn:aws:sqs:us-east-1:950331671021:copy-boss-cuboids-deadletter",
                        "maxReceiveCount": "5",
                    }
                ),
            },
        )
    return queue


def create_cube_metadata(metadata, xx, yy, zz, res, scale_at_res, extent_at_res):
    coll_id = metadata["coll_ids"]
    exp_id = metadata["exp_ids"]
    ch_id = metadata["ch_ids"]
    offset = metadata["offset"]

    cube_metadata = dict(metadata)
    s3key = create_key(xx, yy, zz, coll_id, exp_id, ch_id, res, offset)

    cube_info = {
        "s3key": s3key,
        "x": xx,
        "y": yy,
        "z": zz,
        "res": res,
        "scale_at_res": scale_at_res,  # in nanometers
        "extent_at_res": extent_at_res,
    }

    cube_metadata.update(cube_info)

    return cube_metadata


def return_xyz_keys(offset, extent, cube_size):
    # iterate through the x,y,z
    for xx in range(offset[0], extent[0], cube_size[0]):
        for yy in range(offset[1], extent[1], cube_size[1]):
            for zz in range(offset[2], extent[2], cube_size[2]):
                yield (xx, yy, zz)


def return_messages(metadata):
    """Given metadata about a dataset, generate messages for each cuboid to transfer"""

    offset = metadata["offset"]
    extent = metadata["extent"]

    if metadata["hierarchy_method"] == "isotropic":
        iso = True
    else:
        iso = False

    # iterate over res
    if metadata["downsample_status"] == "DOWNSAMPLED":
        res_levels = metadata["num_hierarchy_levels"]
    else:
        res_levels = 1  # only one level (res 0)

    for res in range(res_levels):  # w/ 4 levels, you have 0,1,2,3
        scale_at_res = ngprecomputed.get_scale_at_res(metadata["scale"], res, iso=iso)
        extent_at_res = ngprecomputed.get_extent_at_res(extent, res, iso=iso)

        for xx, yy, zz in return_xyz_keys(offset, extent_at_res, CUBE_SIZE):
            cube_metadata = create_cube_metadata(
                metadata, xx, yy, zz, res, scale_at_res, extent_at_res
            )
            yield cube_metadata


def create_key(xx, yy, zz, coll_id, exp_id, ch_id, res, offset):
    x_i, y_i, z_i = [i // cubes for i, cubes, o in zip([xx, yy, zz], CUBE_SIZE, offset)]
    mortonid = mortonxyz.XYZMorton(x_i, y_i, z_i)

    s3key = bosslib.ret_boss_key(coll_id, exp_id, ch_id, res, T, mortonid)

    return s3key


def chunks(iterable, size=10):
    """Takes an iterable and chunks it into a generator of chunk size"""
    # from: https://stackoverflow.com/a/24527424/532963

    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


def send_msgs_batch(msg_batch, queue):
    entries = []
    for msg in msg_batch:
        Id = msg["s3key"].split("&")[0]
        entry = {"Id": Id, "MessageBody": json.dumps(msg)}
        entries.append(entry)
    queue.send_messages(Entries=entries)


def send_messages(msgs):
    maxBatchSize = 10  # current maximum allowed

    send_msgs_batch_partial = partial(send_msgs_batch, queue=create_or_get_queue())

    for batch in chunks(msgs, maxBatchSize):
        send_msgs_batch_partial(batch)


def clamp_offset(offset, cube_size=CUBE_SIZE):
    """Returns a clamped offset to the cube size
    """
    return [o // c * c for o, c in zip(offset, cube_size)]


def get_ch_metadata(coll, exp, ch, dest_bucket):
    """given a coll, exp, ch strings
    returns as a dict the row from the csv file with metadata about this channel
    """

    # read and parse the CSV file that contains all the public datasets
    df = pd.read_csv(PUBLIC_METADATA, na_filter=False)
    df = df[(df["coll"] == coll) & (df["exp"] == exp) & (df["ch"] == ch)]
    metadata = df.to_dict(orient="records")[0]

    # generate the path to the precomputed volume
    layer_path = "/".join((metadata["coll"], metadata["exp"], metadata["ch"]))

    metadata["layer_path"] = layer_path
    metadata["dest_bucket"] = dest_bucket
    metadata["path"] = f"s3://{dest_bucket}/{layer_path}/"

    # set some metadata about the channel
    if metadata["dtype"] in ["uint8", "uint16"]:
        metadata["layer_type"] = "image"
        metadata["encoding"] = "raw"
    else:
        # metadata["layer_type"] = "segmentation"
        # metadata["encoding"] = "compressed_segmentation"
        raise ValueError("Segmentations not supported yet")

    # get the scale for res 0 data
    metadata["scale"] = bosslib.get_scale(
        metadata["x_voxel_size"],
        metadata["y_voxel_size"],
        metadata["z_voxel_size"],
        metadata["voxel_unit"],
    )

    # get the extent, offset, and volume size of the data
    metadata["extent"] = metadata["x_stop"], metadata["y_stop"], metadata["z_stop"]
    metadata["offset"] = clamp_offset(
        (metadata["x_start"], metadata["y_start"], metadata["z_start"])
    )

    # volume size is the actual volume that data exists in (like the size of a numpy array)
    metadata["volume_size"] = tuple(
        e - o for e, o in zip(metadata["extent"], metadata["offset"])
    )

    metadata["chunk_size"] = CUBE_SIZE
    metadata["input_cube_size"] = CUBE_SIZE
    metadata["compression"] = COMPRESSION
    metadata["boss_bucket"] = BOSS_BUCKET

    return metadata


@click.command()
@click.argument("coll")
@click.argument("exp")
@click.argument("ch")
@click.argument("dest_bucket")
# "ZBrain", "ZBrain", "ZBB_y385-Cre"
def gen_messages(coll, exp, ch, dest_bucket):

    # get the metadata for this channel
    ch_metadata = get_ch_metadata(coll, exp, ch, dest_bucket)

    # create the precomputed volume
    create_precomputed_volume(S3_RESOURCE, **ch_metadata)

    msgs = return_messages(ch_metadata)

    # iterate through dataset, generating s3keys, and send them to queue
    send_messages(msgs)


if __name__ == "__main__":
    gen_messages()  # pylint: disable=no-value-for-parameter
