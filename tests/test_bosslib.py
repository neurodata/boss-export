from boss_export.libs import bosslib
import boto3

session = boto3.Session(profile_name="boss-s3")


def test_get_boss_data():
    s3resource = session.resource("s3")
    s3Bucket = "cuboids.production.neurodata"
    s3Key = "89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0"
    dtype = "uint8"
    cube_size = 512, 512, 16

    data = bosslib.get_boss_data(s3resource, s3Bucket, s3Key, dtype, cube_size)
    assert data.shape == (16, 512, 512)

