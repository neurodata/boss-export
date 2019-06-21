from boss_export.libs import ndstore


PROJECT = "bock11"
CHANNEL = "image"
TIME_INDEX = 0


def test_returns3key():
    s3key = "0000269c434123d23c0160b33025ba17&bock11&image&0&3090486&0"

    _, x, y, z, res = ndstore.returnxyz(s3key)

    s3key_calc = ndstore.returns3key(PROJECT, CHANNEL, res, TIME_INDEX, x, y, z)

    assert s3key == s3key_calc
