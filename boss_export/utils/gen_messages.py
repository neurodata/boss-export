"""given a channel/experiment/collection
outputs messages in SQS for every cuboid in BOSS
"""

import boto3
import click
import pandas as pd
from botocore.exceptions import ParamValidationError

from boss_export.libs import bosslib, mortonxyz

SESSION = boto3.Session(profile_name="icc")
SQS = SESSION.resource("sqs")
SQS_NAME = "copy-boss-cuboids"

# globals
DEST_BUCKET = "open-neurodata-test"  # testing location

PUBLIC_METADATA = "scripts/public_datasets_scales.csv"

T = 0  # this is always 0
CUBE_SIZE = 512, 512, 16


def send_message(queue, msg):
    queue.send_message(MessageBody=msg)


def create_or_get_queue():
    try:
        queue = SQS.create_queue(
            QueueName=SQS_NAME,
            # Attributes={"DelaySeconds": 5, "ReceiveMessageWaitTimeSeconds": 5},
        )
    except ParamValidationError:
        queue = SQS.get_queue_by_name(QueueName=SQS_NAME)
    return queue


def gen_message(s3key, metadata):
    # dest_layer, scale, cube_size, dtype, extent, offset

    # s3Key
    # dest_dataset = "bock11_test"
    # dest_layer = "image_test"
    # base_scale = 4, 4, 40
    # dtype = "uint8"
    msg = metadata.update({"s3key": s3key})

    return msg


def create_key(xx, yy, zz, coll_id, exp_id, ch_id, res, offset):
    x_i, y_i, z_i = [i // cubes for i, cubes, o in zip([xx, yy, zz], CUBE_SIZE, offset)]
    mortonid = mortonxyz.XYZMorton(x_i, y_i, z_i)

    s3key = bosslib.ret_boss_key(coll_id, exp_id, ch_id, res, T, mortonid)

    return s3key


def send_messages(metadata):
    queue = create_or_get_queue()

    # coll,exp,ch,exp_description,num_hierarchy_levels,dtype,x_start,x_stop,y_start,y_stop,z_start,z_stop,coll_ids,exp_ids,ch_ids
    # kharris15,apical,em,Apical Dendrite Volume,3,uint8,0,8192,0,8192,0,194,4,2,2

    offset = metadata["x_start"], metadata["y_start"], metadata["z_start"]
    extent = metadata["x_stop"], metadata["y_stop"], metadata["z_stop"]
    coll_id = metadata["coll_ids"]
    exp_id = metadata["exp_ids"]
    ch_id = metadata["ch_ids"]

    # iterate over res
    res_levels = metadata["num_hierarchy_levels"]
    for res in range(res_levels):  # w/ 4 levels, you have 0,1,2,3
        # iterate through the x,y,z
        for xx in range(offset[0], extent[0], CUBE_SIZE[0]):
            for yy in range(offset[1], extent[1], CUBE_SIZE[1]):
                for zz in range(offset[2], extent[2], CUBE_SIZE[2]):

                    s3key = create_key(xx, yy, zz, coll_id, exp_id, ch_id, res, offset)

                    msg = gen_message(s3key, metadata)
                    send_message(queue, msg)
                    break


def get_ch_metadata(coll, exp, ch):
    """given a coll, exp, ch strings
    returns as a dict the row from the csv file with metadata about this channel
    """

    df = pd.read_csv(PUBLIC_METADATA, index_col=0)
    df = df[(df["coll"] == coll) & (df["exp"] == exp) & (df["ch"] == ch)]

    return df.to_dict(orient="records")[0]


@click.command()
@click.argument("coll")
@click.argument("exp")
@click.argument("ch")
def gen_messages(coll, exp, ch):

    # get the metadata for this channel
    ch_metadata = get_ch_metadata(coll, exp, ch)

    # assert that we're not doing annotations
    assert ch_metadata["dtype"] != "uint64"

    # iterate through dataset, generating s3keys, and send them to queue
    send_messages(ch_metadata)


if __name__ == "__main__":
    gen_messages()  # pylint: disable=no-value-for-parameter
