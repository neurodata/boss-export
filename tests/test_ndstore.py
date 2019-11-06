import boto3
import numpy as np

from boss_export.libs import mortonxyz, ndstore

PROJECT = "bock11"
CHANNEL = "image"
TIME_INDEX = 0
SESSION = boto3.Session(profile_name="ben-ndstore")
BUCKETNAME = "neurodata-cuboid-store"
BLOCK_SIZE = 512, 512, 64


def test_parts_from_ndstorekey():
    # test we can reverse the keys...
    s3key = "0000269c434123d23c0160b33025ba17&bock11&image&0&3090486&0"

    ndstorekey = ndstore.parts_from_ndstorekey(s3key)

    x, y, z = mortonxyz.MortonXYZ(ndstorekey.mortonid)

    s3key_calc = ndstore.returns3key(
        PROJECT, CHANNEL, ndstorekey.res, TIME_INDEX, x, y, z
    )

    assert s3key == s3key_calc

    s3key = "0000d5e940dcaf9c67fba5d3a7780207&bock11&image&0&4313812&0"
    ndstorekey = ndstore.parts_from_ndstorekey(s3key)

    assert ndstorekey.s3key == s3key
    assert ndstorekey.digest == "0000d5e940dcaf9c67fba5d3a7780207"
    assert ndstorekey.project == "bock11"
    assert ndstorekey.channel == "image"
    assert ndstorekey.res == 0
    assert ndstorekey.t == 0
    assert ndstorekey.mortonid == 4313812


def test_get_block():
    s3_resource = SESSION.resource("s3")

    # no data (zeros)
    s3key = "0000365094beac6518ca2f7f2a3ec53e&bock11&image&0&4733540&0"
    data_array = ndstore.get_block(s3key, s3_resource, BUCKETNAME)
    assert data_array.shape == BLOCK_SIZE[::-1]
    assert data_array.sum() == 0
    assert np.array_equal(np.zeros(BLOCK_SIZE[::-1]), data_array)

    # ~1MB
    s3key = "00018e29280c33476cb2130dcb0bb515&bock11&image&0&21498135&0"
    data_array = ndstore.get_block(s3key, s3_resource, BUCKETNAME)
    assert data_array.sum() > 0
    assert data_array.shape == BLOCK_SIZE[::-1]

    # ~16 MB
    # s3key = "0000d5e940dcaf9c67fba5d3a7780207&bock11&image&0&4313812&0"
    # data_array = ndstore.get_block(s3key, s3_resource, BUCKETNAME)

    # weird cubes:
    # "000049ba5ed95c2d81f43827e9075876&bock11&image&0&1910472&0"
    # mortonxyz.MortonXYZ(1910472)
    # [78, 124, 64]

    # "0000269c434123d23c0160b33025ba17&bock11&image&0&3090486&0"
    # mortonxyz.MortonXYZ(3090486)
    # [192, 115, 43]
