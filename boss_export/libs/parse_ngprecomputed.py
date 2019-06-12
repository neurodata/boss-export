import requests


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


def xyz_cube_idx_from_ng(xyz_act, cube_size, offset):
    xyz_act = [(xyz - o) / c for xyz, c, o in zip(xyz_act, cube_size, offset)]
    return xyz_act


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


def main():
    ngpath = "s3://nd-precomputed-volumes/bock11/image"

    info = get_scales_ngpath(ngpath)
    scales = info["scales"]

    ngkey = "bock11/image/32_32_40/7168-7680_6144-6656_2917-2981"
    xyzrange, voxel_size = parse_ngkey(ngkey)
    res = get_res_from_ngkey(ngkey, scales)

    print("ngkey", ngkey)
    print()


if __name__ == "__main__":
    main()
