import boto3

from boss_export.libs import bosslib, mortonxyz

session = boto3.Session(profile_name="boss-s3")


def test_ret_boss_key():
    coll = 51  # "bock"
    exp = 174  # "bock11"
    ch = 1005  # "image"
    res = 0
    t = 0
    extent = 135424, 119808, 4156
    # offset = 0, 0, 2917
    block_size = 512, 512, 16
    x, y, z = [e // bb for e, bb in zip(extent, block_size)]
    x_2, y_2 = [xy // 2 for xy in [x, y]]
    mortonid = mortonxyz.XYZMorton(x_2, y_2, z)

    key = bosslib.ret_boss_key(coll, exp, ch, res, t, mortonid)
    assert key == "55169619bc8726216df2ed93167fce3a&51&174&1005&0&0&69804262&0"


def test_parts_from_bosskey():
    s3key = "55169619bc8726216df2ed93167fce3a&51&174&1005&0&0&69804262&0"
    bosskey = bosslib.parts_from_bosskey(s3key)

    assert bosskey.s3key == s3key
    assert bosskey.parent_iso is None
    assert bosskey.col_id == 51
    assert bosskey.exp_id == 174
    assert bosskey.chan_id == 1005
    assert bosskey.res == 0
    assert bosskey.t == 0
    assert bosskey.mortonid == 69804262
    assert bosskey.version == 0


def test_get_boss_data():
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

    s3resource = session.resource("s3")
    s3Bucket = "cuboids.production.neurodata"
    s3Key = "89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0"
    dtype = "uint8"
    cube_size = 512, 512, 16

    data = bosslib.get_boss_data(s3resource, s3Bucket, s3Key, dtype, cube_size)
    assert data.shape == cube_size[::-1]
    assert data.dtype == dtype
