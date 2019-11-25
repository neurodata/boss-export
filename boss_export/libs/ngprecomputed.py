import gzip
import math
from functools import partial

import boto3
import brotli
import numpy as np
import requests

from boss_export.libs import chunks, mortonxyz


def parse_ngkey(ngkey):
    # 32_32_40/7168-7680_6144-6656_2917-2981"

    ngkey_parts = ngkey.split("/")

    voxel_size = [float(r) for r in ngkey_parts[2].split("_")]
    xyzrange = [[int(a) for a in xyz.split("-")] for xyz in ngkey_parts[3].split("_")]
    return xyzrange, voxel_size


def get_res_from_ngkey(voxel_size, scales):
    i = 0
    for scale in scales:
        if scale["resolution"] == voxel_size:
            return i
        i += 1
    return None


def xyz_cube_idx_from_xyz_act(xyz_act, cube_size, offset):
    """mortonid in neuroglancer has no offset
    """
    xyz_no_offset = [
        int((xyz - o) / c) for xyz, c, o in zip(xyz_act, cube_size, offset)
    ]

    return xyz_no_offset


def ngmorton(mortonid_actual, cube_size, offset):
    """takes a mortonid w/ offset and removes it
    given that the offset is aligned with cube size
    """
    xyz_idx = mortonxyz.MortonXYZ(mortonid_actual)
    xyz_act = [i * c for i, c in zip(xyz_idx, cube_size)]
    xyz_no_offset = xyz_cube_idx_from_xyz_act(xyz_act, cube_size, offset)

    return mortonxyz.XYZMorton(*xyz_no_offset)


def get_scales_ngpath(ngpath):
    """returns a dictionary describing a neuroglancer volume from its path
    """
    if ngpath.endswith("/info"):
        ngpath = ngpath.split("info")[0]

    if ngpath.startswith("s3://"):
        ngpath = "https://s3.amazonaws.com/" + ngpath.split("s3://")[1]

    infopath = ngpath + "/info"
    r = requests.get(infopath)
    r.raise_for_status()
    info = r.json()

    return info


def numpy_chunk(data_array, compression="gzip"):
    def raw(x):
        return x

    if compression == "gzip":  # gzip
        compress = gzip.compress
    elif compression == "br":  # brotli
        compress = partial(brotli.compress, quality=6)
    elif compression == "":  # raw
        compress = raw
    else:
        raise NotImplementedError("Unsupported compression format")

    if np.dtype(data_array.dtype) in (np.uint8, np.uint16):
        data_encode = data_array.T
        encode = chunks.encode_raw
    else:
        # the compressed seg expects there to be 4D (first dim is channel)
        data_encode = np.expand_dims(data_array, axis=0)
        encode = partial(
            chunks.encode_compressed_segmentation_pure_python, block_size=(8, 8, 8)
        )

    comp_array = compress(encode(data_encode))

    return comp_array


def get_ng_key(dataset, layer, chunk_name):
    if layer:
        ngkey = f"{dataset}/{layer}/{chunk_name}"
    else:
        ngkey = f"{dataset}/{chunk_name}"

    return ngkey


def get_chunk_name(
    mortonid, basescale, res, cube_shape, array_shape, offset=[0, 0, 0], iso=False
):
    """
    this returns neuroglancer cube name (minus the volume info)
    mortonid starts from 0 *without the offset*
    offset is added to the s3key string

    array_shape is zyz order (numpy ordered)

    >> s3key = get_chunk_name(0, (4, 4, 40), 0, (512, 512, 16), (16, 109, 512), (0, 0, 2917))
    """

    # ref FULL key: s3://nd-precomputed-volumes/bock11/image/32_32_40/7168-7680_6144-6656_2917-2981

    xyz = mortonxyz.MortonXYZ(int(mortonid))
    xyz_str = "_".join(
        [
            "-".join([str(i * c + o), str(i * c + o + c + (a - c))])
            for i, c, o, a in zip(xyz, cube_shape, offset, array_shape[::-1])
        ]
    )

    scale = get_scale_at_res(basescale, res, iso)
    scale_str = "_".join([str(s) for s in scale])

    return f"{scale_str}/{xyz_str}"


def crop_to_extent(data, xyz, extent):
    """Limits the data to the volume's extent
    data is z, y, x ordered
    xyz and extent are xyz ordered
    """

    diff_extent = [e - i for e, i in zip(extent, xyz)]

    data_clip = data[: diff_extent[2], : diff_extent[1], : diff_extent[0]]

    return data_clip


def assume_role_resource(iam_role, session):
    # iam_role = "arn:aws:iam::222222222222:role/role-on-source-account"
    # session is either boto3 or a boto3 Session object

    sts_connection = session.client("sts")
    acct_b = sts_connection.assume_role(
        RoleArn=iam_role, RoleSessionName="cross_acct_lambda"
    )

    ACCESS_KEY = acct_b["Credentials"]["AccessKeyId"]
    SECRET_KEY = acct_b["Credentials"]["SecretAccessKey"]
    SESSION_TOKEN = acct_b["Credentials"]["SessionToken"]

    # create service client using the assumed role credentials, e.g. S3
    resource = boto3.resource(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )

    return resource


def save_obj(
    s3_resource,
    bucket,
    ngkey,
    data,
    storage_class="INTELLIGENT_TIERING",
    content_encoding=None,
    cache_control="max-age=3600, s-max-age=3600",
    content_type="application/octet-stream",
    public=True,
    owner_id=None,
):
    """Saves data into a bucket with the parameters needed for neuroglancer
    Returns: dict
    """

    kwargs = {}
    if public:
        kwargs["GrantRead"] = 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

    if owner_id is None:
        if public and bucket in ["open-neurodata", "open-neurodata-test"]:
            # set to open-data-bucket
            kwargs[
                "GrantFullControl"
            ] = "id=7c79f0c2067662c1a9274f7307b10136544223f9f9d4cd1f2d8d8931a04b99a6"
        else:
            kwargs["ACL"] = "bucket-owner-full-control"

    if content_encoding is not None:
        kwargs["ContentEncoding"] = content_encoding

    obj = s3_resource.Object(bucket, ngkey)
    resp = obj.put(
        Body=data,
        StorageClass=storage_class,
        CacheControl=cache_control,
        ContentType=content_type,
        **kwargs,
    )

    return resp


def get_scale_at_res(base_scale, res, iso=False):
    """Returns voxel resolution at a given resolution of precomptued volume
    res=0 is base resolution

    Raises: ValueError if any element in base_scale < 1
    """
    try:
        assert all([s >= 1 for s in base_scale])
    except AssertionError:
        raise ValueError("voxel resolution is < 1")

    if iso:
        factor = (2, 2, 2)
    else:
        factor = (2, 2, 1)

    scale = [s * f ** res for s, f in zip(base_scale, factor)]

    return [int(s) if s == int(s) else s for s in scale]


def get_extent_at_res(base_extent, res, iso=False):
    """Returns extent at a given resolution of precomptued volume
    res=0 is base resolution
    """

    if iso:
        factor = (2, 2, 2)
    else:
        factor = (2, 2, 1)

    return [math.ceil(e / f ** res) for e, f in zip(base_extent, factor)]
