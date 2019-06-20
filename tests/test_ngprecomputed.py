import boto3
import numpy as np
from pytest import raises
from requests import exceptions

from boss_export.libs import bosslib, ngprecomputed


def test_get_scales_ngpath():
    ngpath = "s3://nd-precomputed-volumes/bock11/image"
    info = ngprecomputed.get_scales_ngpath(ngpath)
    assert info["data_type"] == "uint8"
    assert info["num_channels"] == 1
    assert info["type"] == "image"
    assert "scales" in info

    ngpath_broken = "gobblygook"
    assert raises(
        exceptions.MissingSchema, ngprecomputed.get_scales_ngpath, ngpath_broken
    )


def test_get_scale():
    scales = [
        [4, 4, 40],
        [8, 8, 40],
        [16, 16, 40],
        [32, 32, 40],
        [64, 64, 40],
        [128, 128, 40],
        [256, 256, 40],
        [512, 512, 40],
        [1024, 1024, 40],
        [2048, 2048, 40],
    ]

    for res in range(0, 10):
        assert scales[res] == ngprecomputed.get_scale(scales[0], res)

    for res in range(0, 10):
        assert scales[res][0:2] + [
            scales[res][2] * 2 ** res
        ] == ngprecomputed.get_scale(scales[0], res, iso=True)


def test_get_key():
    basescale = [4, 4, 40]
    res = 3
    shape = [512, 512, 16]
    offset = [0, 0, 2917]

    mortonid = 0
    ngkey = ngprecomputed.get_key(mortonid, basescale, res, shape, offset)
    assert "32_32_40/0-512_0-512_2917-2933" == ngkey

    mortonid = 14
    # xyz = mortonxyz.MortonXYZ(mortonid)
    # xyz = [2,1,1]

    res = 0
    ngkey = ngprecomputed.get_key(mortonid, basescale, res, shape, offset)
    assert f"4_4_40/{512*2}-{512*3}_{512*1}-{512*2}_{2917+16}-{2917+16*2}" == ngkey


def test_limit_to_extent():
    # bock11 extents
    extent = 135424, 119808, 4156  # xyz order

    xyz = 135168, 119296, 4144  # xyz order

    # this is the shape it comes back from when reading it out of boss
    data_full = np.zeros((16, 512, 512), dtype="uint8", order="F")  # zyx order

    data_limit = ngprecomputed.crop_to_extent(data_full, xyz, extent)

    limited_shape = tuple(e - i for e, i in zip(extent, xyz))

    assert data_limit.shape == limited_shape[::-1]


def test_save_obj():
    # chan_id:1005
    # col_id:51
    # digest:'89bb785630a9446b6a564c8779b3678d'
    # exp_id:174
    # mortonid:12282054
    # parent_iso:None
    # res:0
    # s3key:'89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0'
    # t:0
    # version:0

    # mortonxyz.MortonXYZ(12282054)
    # [132, 117, 249]

    session = boto3.Session(profile_name="boss-s3")
    boss_s3resource = session.resource("s3")
    boss_s3Bucket = "cuboids.production.neurodata"
    boss_s3Key = "89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0"
    dtype = "uint8"
    cube_size = 512, 512, 16
    offset = [0, 0, 2917]

    basescale = [4, 4, 40]

    bosskey = bosslib.parts_from_bosskey(boss_s3Key)
    ngkey = ngprecomputed.get_key(
        bosskey.mortonid, basescale, bosskey.res, cube_size, offset
    )

    data = bosslib.get_boss_data(
        boss_s3resource, boss_s3Bucket, boss_s3Key, dtype, cube_size
    )

    # save it to a test folder
    ng_session = boto3.Session(profile_name="NGuser")
    ng_s3resource = ng_session.resource("s3")
    ng_s3Bucket = "nd-precomputed-volumes"

    ngprecomputed.save_obj(ng_s3resource, ng_s3Bucket, ngkey, data)

    # attempt to read it back in?

    # examine dict to determine what params we can check
