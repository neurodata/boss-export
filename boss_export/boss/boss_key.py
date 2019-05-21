import hashlib


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
