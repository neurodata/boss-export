"""given a channel/experiment/collection
outputs messages in SQS for every cuboid in BOSS
"""

import itertools
import json
import math
import os

import boto3
import click
import pandas as pd
from botocore.exceptions import ParamValidationError

from boss_export.libs import bosslib, mortonxyz
from cloudvolume import CloudVolume

SESSION = boto3.Session(profile_name="icc")

# if we set env. variables,
# and it doesn't find the secret in ~/.cloudvolume
# then cloudvolume uses them
credentials = SESSION.get_credentials()
os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key


SQS = SESSION.resource("sqs", region_name="us-east-1")
SQS_NAME = "copy-boss-cuboids"

# globals
DEST_BUCKET = "open-neurodata-test"  # testing location
# DEST_BUCKET = "open-neurodata"  # actual location

PUBLIC_METADATA = "scripts/public_datasets_downsample.csv"

T = 0  # this is always 0
CUBE_SIZE = 512, 512, 16  # constant for BOSS


def create_or_get_queue():
    try:
        queue = SQS.create_queue(
            QueueName=SQS_NAME,
            # Attributes={"DelaySeconds": 5, "ReceiveMessageWaitTimeSeconds": 5},
        )
    except ParamValidationError:
        queue = SQS.get_queue_by_name(QueueName=SQS_NAME)
    return queue


def create_cube_metadata(metadata, xx, yy, zz, res, scale_at_res, extent_at_res):
    coll_id = metadata["coll_ids"]
    exp_id = metadata["exp_ids"]
    ch_id = metadata["ch_ids"]
    offset = metadata["x_start"], metadata["y_start"], metadata["z_start"]

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

    # coll,exp,ch,exp_description,num_hierarchy_levels,dtype,x_start,x_stop,y_start,y_stop,z_start,z_stop,coll_ids,exp_ids,ch_ids
    # kharris15,apical,em,Apical Dendrite Volume,3,uint8,0,8192,0,8192,0,194,4,2,2

    offset = metadata["x_start"], metadata["y_start"], metadata["z_start"]
    extent = metadata["x_stop"], metadata["y_stop"], metadata["z_stop"]

    # iterate over res
    if metadata["downsample_status"] == "DOWNSAMPLED":
        res_levels = metadata["num_hierarchy_levels"]
    else:
        res_levels = 1  # only one level (res 0)

    for res in range(res_levels):  # w/ 4 levels, you have 0,1,2,3
        scale_at_res = [s * 2 ** res for s in metadata["scale"][0:2]] + [
            metadata["scale"][2]
        ]
        extent_at_res = [math.ceil(e / 2 ** res) for e in extent[0:2]] + [extent[2]]

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


def send_messages(msgs):
    queue = create_or_get_queue()
    maxBatchSize = 10  # current maximum allowed

    for chunk in chunks(msgs, maxBatchSize):
        entries = []
        for x in chunk:
            Id = x["s3key"].split("&")[0]
            entry = {"Id": Id, "MessageBody": json.dumps(x)}
            entries.append(entry)
        queue.send_messages(Entries=entries)


def get_ch_metadata(coll, exp, ch):
    """given a coll, exp, ch strings
    returns as a dict the row from the csv file with metadata about this channel
    """

    # read and parse the CSV file that contains all the public datasets
    df = pd.read_csv(PUBLIC_METADATA)
    df = df[(df["coll"] == coll) & (df["exp"] == exp) & (df["ch"] == ch)]
    metadata = df.to_dict(orient="records")[0]

    # generate the path to the precomputed volume
    layer_path = "/".join((metadata["coll"], metadata["exp"], metadata["ch"]))

    metadata["layer_path"] = layer_path
    metadata["dest_bucket"] = DEST_BUCKET
    metadata["path"] = f"s3://{DEST_BUCKET}/{layer_path}/"

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
    metadata["offset"] = metadata["x_start"], metadata["y_start"], metadata["z_start"]

    # volume size is the actual volume that data exists in (like the size of a numpy array)
    metadata["volume_size"] = tuple(
        e - o for e, o in zip(metadata["extent"], metadata["offset"])
    )

    return metadata


def create_precomputed_volume(metadata):
    """Use CloudVolume to create the precomputed info file"""

    info = CloudVolume.create_new_info(
        num_channels=1,
        layer_type=metadata["layer_type"],
        data_type=metadata["dtype"],  # Channel images might be 'uint8'
        encoding=metadata[
            "encoding"
        ],  # raw, jpeg, compressed_segmentation, fpzip, kempressed
        resolution=metadata["scale"],  # Voxel scaling, units are in nanometers
        voxel_offset=metadata["offset"],  # x,y,z offset in voxels from the origin
        # Pick a convenient size for your underlying chunk representation
        # Powers of two are recommended, doesn't need to cover image exactly
        chunk_size=CUBE_SIZE,  # units are voxels
        volume_size=metadata["extent"],  # units are voxels
    )

    # this requires write access to the bucket
    vol = CloudVolume(metadata["path"], info=info)

    if metadata["downsample_status"] == "DOWNSAMPLED":
        res_levels = metadata["num_hierarchy_levels"]
    else:
        res_levels = 1  # only one level (res 0)

    for res in range(1, res_levels):
        vol.add_scale((2 ** res, 2 ** res, 1), chunk_size=(512, 512, 16))

    vol.commit_info()


@click.command()
@click.argument("coll")
@click.argument("exp")
@click.argument("ch")
# "ZBrain", "ZBrain", "ZBB_y385-Cre"
def gen_messages(coll, exp, ch):

    # get the metadata for this channel
    ch_metadata = get_ch_metadata(coll, exp, ch)

    # create the precomputed volume
    create_precomputed_volume(ch_metadata)

    msgs = return_messages(ch_metadata)

    # iterate through dataset, generating s3keys, and send them to queue
    send_messages(msgs)


if __name__ == "__main__":
    gen_messages()  # pylint: disable=no-value-for-parameter
