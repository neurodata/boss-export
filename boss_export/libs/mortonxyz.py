"""
methods to calculate morton index
"""

import morton  # https://pypi.python.org/pypi/morton-py/1.2


def XYZMorton(x, y, z):
    """ Get morton order from XYZ coordinates """
    m = morton.Morton(dimensions=3, bits=64)
    return m.pack(x, y, z)


def MortonXYZ(zindex):
    """ Get XYZ coordinates from morton order """
    m = morton.Morton(dimensions=3, bits=64)
    return m.unpack(zindex)


def get_coords(mortonid, cube_size):
    """ Given a morton id and cube size, return voxel position in the volume (xyz)
    """
    xyz = MortonXYZ(mortonid)
    xyz_coords = [i * c for i, c in zip(xyz, cube_size)]

    return xyz_coords
