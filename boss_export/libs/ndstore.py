import hashlib

from boss_export.libs import mortonxyz


def calc_s3_key(project, channel, morton_index, res, time_index):
    hashm = hashlib.md5()

    end_str = "{}&{}&{}&{}&{}".format(project, channel, res, morton_index, time_index)

    hashm.update(end_str.encode("utf-8"))
    hashhex = hashm.hexdigest()
    s3key = "{}&{}".format(hashhex, end_str)
    return s3key


def returns3key(project, channel, res, time_index, x, y, z):
    m_idx = mortonxyz.XYZMorton(x, y, z)
    return calc_s3_key(project, channel, m_idx, res, time_index)


def returnxyz(s3key):
    # returns list x,y,z,res

    s3key_parts = s3key.split("&")

    res = int(s3key_parts[3])

    morton_idx = int(s3key_parts[4])

    x, y, z = mortonxyz.MortonXYZ(morton_idx)

    return morton_idx, x, y, z, res

