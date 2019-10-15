import gzip

import boto3
import brotli
import numpy as np
import pytest
from pytest import raises
from requests import exceptions

from boss_export.libs import bosslib, mortonxyz, ngprecomputed


#! failing because offset not aligned with the cube size
@pytest.mark.xfail
def test_ngmorton():
    cube_size = 512, 512, 16
    offset = 0, 0, 2917

    boss_xyz_idx = [o // c for o, c in zip(offset, cube_size)]

    mortonid_actual = mortonxyz.XYZMorton(*boss_xyz_idx)

    ng_morton = ngprecomputed.ngmorton(mortonid_actual, cube_size, offset)
    assert ng_morton == 0


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


def test_ng_key():
    dataset = "bock11_test"
    layer = "image_test"
    chunk_name = "32_32_40/0-512_0-512_2917-2933"

    s3_key = ngprecomputed.get_ng_key(dataset, layer, chunk_name)

    assert s3_key == "bock11_test/image_test/32_32_40/0-512_0-512_2917-2933"


def test_get_chunk_name():
    basescale = 4, 4, 40
    res = 3
    cube_shape = 512, 512, 16
    array_shape = 16, 512, 512
    offset = 0, 0, 2917

    mortonid = 0
    chunk_name = ngprecomputed.get_chunk_name(
        mortonid, basescale, res, cube_shape, array_shape, offset
    )
    assert "32_32_40/0-512_0-512_2917-2933" == chunk_name

    mortonid = 14
    # xyz = mortonxyz.MortonXYZ(mortonid)
    # xyz = [2,1,1]

    res = 0
    chunk_name = ngprecomputed.get_chunk_name(
        mortonid, basescale, res, cube_shape, array_shape, offset
    )
    assert f"4_4_40/{512*2}-{512*3}_{512*1}-{512*2}_{2917+16}-{2917+16*2}" == chunk_name


def test_get_chunk_name_crop():
    # ,295,[512, 512, 11cube_shaped63f2fa28ecd8ef1a1884a787&36&116&869&0&0&295&0,[798.0, 798.0, 2000.0],(16, 109, 512)
    # extent = [1406, 621, 138]

    mortonid = 295
    basescale = [798.0, 798.0, 2000.0]
    array_shape = (16, 109, 512)
    cube_shape = (512, 512, 16)
    offset = [0, 0, 0]
    res = 0
    chunk_name = ngprecomputed.get_chunk_name(
        mortonid, basescale, res, cube_shape, array_shape, offset
    )

    chunk_name_correct = "798.0_798.0_2000.0/512-1024_512-621_112-128"

    assert chunk_name == chunk_name_correct


def test_limit_to_extent():
    # bock11 extents
    extent = 135424, 119808, 4156  # xyz order

    xyz = 135168, 119296, 4144  # xyz order

    # this is the shape it comes back from when reading it out of boss
    data_full = np.zeros((16, 512, 512), dtype="uint8", order="F")  # zyx order

    data_limit = ngprecomputed.crop_to_extent(data_full, xyz, extent)

    limited_shape = tuple(e - i for e, i in zip(extent, xyz))

    assert data_limit.shape == limited_shape[::-1]


def test_numpy_chunk():
    data_array = np.random.randint(0, 2500, (16, 512, 512), "uint16")

    # test no compression
    bstring = ngprecomputed.numpy_chunk(data_array, compression="")
    assert bstring == np.transpose(data_array, (2, 1, 0)).tobytes("F")

    with raises(NotImplementedError):
        ngprecomputed.numpy_chunk(data_array, compression="lz4")

    bstring_gzip = ngprecomputed.numpy_chunk(data_array, compression="gzip")
    bstring_gzip_test = gzip.compress(np.transpose(data_array, (2, 1, 0)).tobytes("F"))
    assert gzip.decompress(bstring_gzip) == gzip.decompress(bstring_gzip_test)

    bstring_br = ngprecomputed.numpy_chunk(data_array, compression="br")
    bstring_br_test = brotli.compress(
        np.transpose(data_array, (2, 1, 0)).tobytes("F"), quality=6
    )
    assert brotli.decompress(bstring_br) == brotli.decompress(bstring_br_test)


#! failing because offset not aligned with the cube size
@pytest.mark.xfail
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
    offset = 0, 0, 2917

    basescale = 4, 4, 40
    dest_dataset = "bock11_test"
    dest_layer = "image_test"

    bosskey = bosslib.parts_from_bosskey(boss_s3Key)

    ngmorton = ngprecomputed.ngmorton(bosskey.mortonid, cube_size, offset)

    chunk_name = ngprecomputed.get_chunk_name(
        ngmorton, basescale, bosskey.res, cube_size, offset
    )
    ngkey = ngprecomputed.get_ng_key(dest_dataset, dest_layer, chunk_name)

    data = bosslib.get_boss_data(
        boss_s3resource, boss_s3Bucket, boss_s3Key, dtype, cube_size
    )

    # save it to a test folder
    ng_session = boto3.Session(profile_name="NGuser")
    ng_s3resource = ng_session.resource("s3")
    ng_s3Bucket = "nd-precomputed-volumes"

    resp = ngprecomputed.save_obj(ng_s3resource, ng_s3Bucket, ngkey, data)

    # attempt to read it back in?

    # examine dict to determine what params we can check
