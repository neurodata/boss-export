import json

from boss_export.lambda_function import s3_batch_boss_export_neuroglancer
from boss_export.utils import gen_messages


def test_convert_cuboid():
    ch_metadata = gen_messages.get_ch_metadata("ZBrain", "ZBrain", "ZBB_y385-Cre")

    xx, yy, zz, res, scale_at_res, cube_scale = (
        2,
        0,
        0,
        0,
        [798.0, 798.0, 2000.0],
        [512, 512, 16],
    )
    msg_dict = gen_messages.create_cube_metadata(
        ch_metadata, xx, yy, zz, res, scale_at_res, cube_scale
    )
    msg_json = json.dumps(msg_dict)
    msg = json.loads(msg_json)

    # convertion to JSON turns tuples into lists
    assert msg.keys() == msg_dict.keys()

    s3_batch_boss_export_neuroglancer.convert_cuboid(msg)
