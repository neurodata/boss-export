"""given a channel/experiment/collection
outputs messages in SQS for every cuboid in BOSS
"""

import click

import boto3
from botocore.exceptions import ParamValidationError


SESSION = boto3.Session(profile_name="icc")

SQS = SESSION.resource("sqs")

SQS_NAME = "copy-boss-cuboids"

# globals
DEST_BUCKET = "open-neurodata-test"  # testing location


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


def create_messages():

    # dest_layer, scale, cube_size, dtype, extent, offset

    # message contents

    # s3Key

    # DEST_DATASET = "bock11_test"
    # DEST_LAYER = "image_test"
    # BASE_SCALE = 4, 4, 40
    # CUBE_SIZE = 512, 512, 16
    # dtype = "uint8"
    # EXTENT = 135424, 119808, 4156  # x, y, z
    # OFFSET = 0, 0, 2917

    # coll,exp,ch,exp_description,num_hierarchy_levels,dtype,x_start,x_stop,y_start,y_stop,z_start,z_stop,coll_ids,exp_ids,ch_ids
    # kharris15,apical,em,Apical Dendrite Volume,3,uint8,0,8192,0,8192,0,194,4,2,2

    # need a generator here to make the messages

    pass


@click.command()
@click.argument("coll")
@click.argument("exp")
@click.argument("ch")
def gen_messages(coll, exp, ch):

    queue = create_or_get_queue()
    msgs = create_messages()
    for msg in msgs:
        send_message(queue, msg)


if __name__ == "__main__":
    gen_messages()  # pylint: disable=no-value-for-parameter
