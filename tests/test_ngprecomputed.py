from pytest import raises
from requests import exceptions

from boss_export.libs import ngprecomputed


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
