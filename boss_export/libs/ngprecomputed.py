import gzip

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
    xyz_no_offset = [(xyz - o) / c for xyz, c, o in zip(xyz_act, cube_size, offset)]

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


def get_scale(base_scale, res, iso=False):
    """base_scale is [x, y, z] voxel sizes in nm at res 0
    returns: scale string at res
    >> get_scale([4, 4, 40], 3)
    [32,32,40]
    """

    if iso:
        scale = [s * 2 ** res for s in base_scale]
    else:
        scale = [s * 2 ** res for s in base_scale[0:2]] + [base_scale[2]]

    return scale


def numpy_chunk(data_array):
    comp_array = gzip.compress(chunks.encode_raw(data_array))
    return comp_array


def get_ng_key(dataset, layer, chunk_name):
    ngkey = f"{dataset}/{layer}/{chunk_name}"

    return ngkey


def get_chunk_name(mortonid, basescale, res, shape, offset=[0, 0, 0], iso=False):
    """
    this returns neuroglancer cube name (minus the volume info)
    mortonid starts from 0 *without the offset*
    offset is added to the s3key string
    >> s3key = get_chunk_name(0, (4, 4, 40), 0, (512, 512, 16), (0, 0, 2917))
    >> s3key
    "4_4_40/0-512_0-512_2917-2933"
    """

    # ref FULL key: s3://nd-precomputed-volumes/bock11/image/32_32_40/7168-7680_6144-6656_2917-2981

    xyz = mortonxyz.MortonXYZ(int(mortonid))
    xyz_str = "_".join(
        [
            "-".join([str(i * c + o), str(i * c + o + c)])
            for i, c, o in zip(xyz, shape, offset)
        ]
    )

    scale = get_scale(basescale, res, iso)
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


def save_obj(s3_resource, bucket, ngkey, data):
    """Saves data into a bucket with the parameters needed for neuroglancer
    Returns: dict
    """
    obj = s3_resource.Object(bucket, ngkey)
    resp = obj.put(
        Body=data,
        StorageClass="INTELLIGENT_TIERING",
        CacheControl="max-age=3600, s-max-age=3600",
        ContentEncoding="gzip",
        ContentType="application/octet-stream",
        ACL="bucket-owner-full-control",
    )

    return resp
