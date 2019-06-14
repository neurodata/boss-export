import hashlib
import blosc
import numpy as np


def HashedKey(*args, version=None):
    """ BOSS Key creation function

    Takes a list of different key string elements, joins them with the '&' char,
    and prepends the MD5 hash of the key to the key.

    Args (Common usage):
        parent_iso (None or 'ISO')
        collection_id
        experiment_id
        channel_id
        resolution
        time_sample
        morton (str): Morton ID of cube

    Keyword Args:
        version : Optional Object version, not part of the hashed value

    Example:
        use_iso_key (boolean) : If the BOSS keys should include an 'ISO=' flag
        iso = 'ISO' if use_iso_key else None
        parent_iso = None if args['resolution'] == args['iso_resolution'] else iso

        >>> parent_iso = None
        >>> col_id=48
        >>> exp_id = 168
        >>> chan_id = 994
        >>> res = 0
        >>> t=0
        >>> mortonid = 21117301
        >>> ver = 0
        >>> print(HashedKey(parent_iso, col_id, exp_id, chan_id, res, t, mortonid, version=ver))
        00000004f98cd89f2034b4a78b5a4558&48&168&994&0&0&21117301&0
    """
    key = "&".join([str(arg) for arg in args if arg is not None])
    digest = hashlib.md5(key.encode()).hexdigest()
    key = "{}&{}".format(digest, key)
    if version is not None:
        key = "{}&{}".format(key, version)
    return key


def ret_boss_key(col_id, exp_id, chan_id, res, t, mortonid, version=0, parent_iso=None):
    # helper function to return the s3 key inside BOSS
    return HashedKey(
        parent_iso, col_id, exp_id, chan_id, res, t, mortonid, version=version
    )


def parts_from_bosskey(s3key):
    """Returns: parent_iso, col_id, exp_id, chan_id, res, t, mortonid, version
    """
    s3keyparts = s3key.split("&")
    return s3keyparts[1:]


def get_boss_data(s3resource, s3Bucket, s3Key, dtype, cube_size):
    """returns data from boss
    >> data = get_boss_data(session.resource("s3"), "cuboids.production.neurodata", "89bb785630a9446b6a564c8779b3678d&51&174&1005&0&0&12282054&0", "uint8", (512, 512, 16) )
    >> data.shape
    (16, 512, 512)
    """
    obj = s3resource.Object(s3Bucket, s3Key)
    r = obj.get()
    rawdata = blosc.decompress(r["Body"].read())
    data = np.frombuffer(rawdata, dtype=dtype)

    return data.reshape(cube_size[::-1])
