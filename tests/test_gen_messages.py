from boss_export.utils import gen_messages


def test_msg_creation():
    # {
    #     "coll": "ZBrain",
    #     "exp": "ZBrain",
    #     "ch": "ZBB_y385-Cre",
    #     "exp_description": nan,
    #     "num_hierarchy_levels": 4,
    #     "dtype": "uint16",
    #     "x_start": 0,
    #     "x_stop": 1406,
    #     "y_start": 0,
    #     "y_stop": 621,
    #     "z_start": 0,
    #     "z_stop": 138,
    #     "coll_ids": 36,
    #     "exp_ids": 116,
    #     "ch_ids": 869,
    #     "x_voxel_size": 798.0,
    #     "y_voxel_size": 798.0,
    #     "z_voxel_size": 2000.0,
    #     "voxel_unit": "nanometers",
    #     "downsample_status": "NOT_DOWNSAMPLED",
    #     "path": "s3://open-neurodata-test/ZBrain/ZBrain/ZBB_y385-Cre/",
    #     "layer_type": "image",
    #     "encoding": "raw",
    #     "scale": (798.0, 798.0, 2000.0),
    #     "extent": (1406, 621, 138),
    #     "offset": (0, 0, 0),
    #     "volume_size": (1406, 621, 138),
    #     "s3key": "7601f90d0c5b14ebbe436451699f3922&36&116&869&0&0&0&0",
    #     "x": 0,
    #     "y": 0,
    #     "z": 0,
    #     "res": 0,
    #     "scale_at_res": [798.0, 798.0, 2000.0],
    #     "extent_at_res": [],
    # }

    expected_keys = [
        "coll",
        "exp",
        "ch",
        "exp_description",
        "num_hierarchy_levels",
        "dtype",
        "x_start",
        "x_stop",
        "y_start",
        "y_stop",
        "z_start",
        "z_stop",
        "coll_ids",
        "exp_ids",
        "ch_ids",
        "x_voxel_size",
        "y_voxel_size",
        "z_voxel_size",
        "voxel_unit",
        "downsample_status",
        "path",
        "layer_type",
        "encoding",
        "scale",
        "extent",
        "offset",
        "volume_size",
        "s3key",
        "x",
        "y",
        "z",
        "res",
        "scale_at_res",
        "extent_at_res",
    ]

    ch_metadata = gen_messages.get_ch_metadata("ZBrain", "ZBrain", "ZBB_y385-Cre")

    xx, yy, zz, res, scale_at_res, extent_at_res = (
        0,
        0,
        0,
        0,
        [798.0, 798.0, 2000.0],
        ch_metadata["extent_at_res"],
    )
    msg = gen_messages.create_cube_metadata(
        ch_metadata, xx, yy, zz, res, scale_at_res, extent_at_res
    )

    assert list(msg.keys()) == expected_keys
