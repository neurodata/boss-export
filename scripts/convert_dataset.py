# import json
from multiprocessing import Pool

import click

from boss_export.lambda_function import s3_batch_boss_export_neuroglancer
from boss_export.libs import ngprecomputed

# from boss_export.libs import bosslib, mortonxyz, ngprecomputed

from boss_export.utils import gen_messages

import boto3

# from botocore.errorfactory import ClientError


# SESSION = boto3.Session(profile_name="boss-s3")
# S3_CLIENT = SESSION.client("s3")
# BOSS_BUCKET = "cuboids.production.neurodata"

SESSION = boto3.Session(profile_name="icc")
S3_RESOURCE = SESSION.resource("s3")


@click.command()
@click.argument("coll")
@click.argument("exp")
@click.argument("ch")
# "ZBrain", "ZBrain", "ZBB_y385-Cre"
def convert_dataset(coll, exp, ch):
    ch_metadata = gen_messages.get_ch_metadata(coll, exp, ch)
    ngprecomputed.create_precomputed_volume(S3_RESOURCE, **ch_metadata)
    msgs = gen_messages.return_messages(ch_metadata)

    # process the messages in parallel
    with Pool() as pool:
        pool.map(
            s3_batch_boss_export_neuroglancer.convert_cuboid,
            msgs
            # (m for m in msgs if m["res"] > 0),
        )

    # processing messages not in parallel with a try/catch (for testing)
    # for msg in msgs:
    #     if msg["res"] < 3:
    #         continue
    #     try:
    #         s3_batch_boss_export_neuroglancer.convert_cuboid(msg)
    #     except Exception:
    #         print("Failed", msg["s3key"])

    # testing to see if all objects exist in BOSS
    # i = 0
    # for msg in msgs:
    #     s3key = msg["s3key"]
    #     try:
    #         S3_CLIENT.head_object(Bucket=BOSS_BUCKET, Key=s3key)
    #     except ClientError:
    #         # Not found
    #         print(s3key, "not found")
    #     i += 1
    # print(i, "messages processed")

    # dumping messages to a file for examination
    # with open(f"{coll}_{exp}_{ch}_msgs.txt", "w") as msg_file:
    #     for msg in msgs:
    #         msg_json = json.dumps(msg)
    #         msg_file.write(f"{msg_json}\n")

    # with open(f"{coll}_{exp}_{ch}_intermediates.csv", "w") as msg_file:
    #     msg_file.write(
    #         "chunk_name,ngmorton,x_coords,y_coords,z_coords,s3Key,scale,shape0,shape1,shape2,extentx,extenty,extentz\n"
    #     )
    #     for msg in msgs:
    #         s3Key = msg["s3key"]
    #         dtype = msg["dtype"]
    #         extent = msg["extent"]
    #         offset = msg["offset"]
    #         # scale = msg["scale"]  # for res 0
    #         scale = msg["scale_at_res"]

    #         bosskey = bosslib.parts_from_bosskey(s3Key)

    #         data_array = bosslib.get_boss_data(
    #             s3_batch_boss_export_neuroglancer.S3_RESOURCE,
    #             s3_batch_boss_export_neuroglancer.BOSS_BUCKET,
    #             s3Key,
    #             dtype,
    #             s3_batch_boss_export_neuroglancer.CUBE_SIZE,
    #         )

    #         xyz_coords = mortonxyz.get_coords(
    #             bosskey.mortonid, s3_batch_boss_export_neuroglancer.CUBE_SIZE
    #         )

    #         data_array = ngprecomputed.crop_to_extent(data_array, xyz_coords, extent)

    #         ngmorton = ngprecomputed.ngmorton(
    #             bosskey.mortonid, s3_batch_boss_export_neuroglancer.CUBE_SIZE, offset
    #         )
    #         chunk_name = ngprecomputed.get_chunk_name(
    #             ngmorton,
    #             scale,
    #             bosskey.res,
    #             s3_batch_boss_export_neuroglancer.CUBE_SIZE,
    #             data_array.shape,
    #             offset,
    #         )

    #         msg_file.write(
    #             f"{chunk_name},{ngmorton},{xyz_coords},{s3Key},{scale},{data_array.shape},{extent}\n"
    #         )


if __name__ == "__main__":
    convert_dataset()  # pylint: disable=no-value-for-parameter
