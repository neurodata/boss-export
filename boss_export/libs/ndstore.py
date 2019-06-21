import hashlib
import time
from dataclasses import dataclass

import blosc
from botocore.exceptions import ClientError

from boss_export.libs import mortonxyz


def calc_s3_key(project, channel, morton_index, res, time_index):
    """Returns an s3 key from morton_index and project params
    """
    hashm = hashlib.md5()

    end_str = "{}&{}&{}&{}&{}".format(project, channel, res, morton_index, time_index)

    hashm.update(end_str.encode("utf-8"))
    hashhex = hashm.hexdigest()
    s3key = "{}&{}".format(hashhex, end_str)
    return s3key


def returns3key(project, channel, res, time_index, x, y, z):
    """Returns an s3 key from xyz and project params
    """
    m_idx = mortonxyz.XYZMorton(x, y, z)
    return calc_s3_key(project, channel, m_idx, res, time_index)


@dataclass
class NdstoreKey:
    s3key: str
    digest: str
    project: str
    channel: str
    res: int
    mortonid: int
    t: int


def parts_from_ndstorekey(s3key):
    """Returns: NdstoreKey
    Note: parent_iso not returned if not in original s3key
    REF:
    s3key = "0000269c434123d23c0160b33025ba17&bock11&image&0&3090486&0"
    HASH_PROJECT_CHANNEL_RES_MORTON_TIME
    """
    s3keyparts = s3key.split("&")

    assert len(s3keyparts) == 6

    # pop the strings
    digest = s3keyparts.pop(0)
    project = s3keyparts.pop(0)
    channel = s3keyparts.pop(0)

    # rest of args are integers (res, mortonid, t)
    s3keyparts = [int(p) for p in s3keyparts]

    ndstorekey = NdstoreKey(s3key, digest, project, channel, *s3keyparts)
    return ndstorekey


def get_block(s3key, s3_resource, bucketname):
    """Gets a cuboid from ndstore
    s3_resource is a boto3 s3 resource
    """
    try:
        s3_obj = s3_resource.Object(bucketname, s3key)
        response = s3_obj.get()
        blosc_data = response["Body"].read()

        # unpack the blosc encoded data
        return blosc.unpack_array(blosc_data, encoding="latin1")

    except ClientError:
        return None
