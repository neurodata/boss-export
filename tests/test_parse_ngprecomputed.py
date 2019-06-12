from boss_export.libs import parse_ngprecomputed
from pytest import raises
from requests import exceptions


def test_get_scales_ngpath():
    ngpath = "s3://nd-precomputed-volumes/bock11/image"
    info = parse_ngprecomputed.get_scales_ngpath(ngpath)
    assert info["data_type"] == "uint8"
    assert info["num_channels"] == 1
    assert info["type"] == "image"
    assert "scales" in info

    ngpath_broken = "gobblygook"
    assert raises(
        exceptions.MissingSchema, parse_ngprecomputed.get_scales_ngpath, ngpath_broken
    )

