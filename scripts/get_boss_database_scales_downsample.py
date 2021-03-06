"""dumped the rds database and instantiated it locally
then iterated over public datasets to get their database IDs
which are needed to determine their S3 keys"""


import mysql.connector
import pandas as pd

HOST = "localhost"
USER = "ben"
PASSWD = ""
DATABASE = "boss"

mydb = mysql.connector.connect(host=HOST, user=USER, passwd=PASSWD, database=DATABASE)
mycursor = mydb.cursor()

df = pd.read_csv("scripts/public_datasets_ids.csv")

coll_ids = []
exp_ids = []
ch_ids = []

x_voxel_sizes = []
y_voxel_sizes = []
z_voxel_sizes = []
voxel_units = []
hierarchy_methods, downsample_statuses, emails = [], [], []

for index, row in df.iterrows():
    exp_id = row["exp_ids"]
    ch_id = row["ch_ids"]

    mycursor.execute(
        f"""select experiment.name,coordinate_frame.name,x_voxel_size,y_voxel_size,z_voxel_size,voxel_unit,experiment.hierarchy_method,downsample_status,auth_user.email 
        from coordinate_frame 
        inner join experiment on coordinate_frame.id = experiment.coord_frame_id 
        inner join channel 
        inner join auth_user on channel.creator_id = auth_user.id 
        where experiment.id = {exp_id} and channel.id = {ch_id}"""
    )

    results = mycursor.fetchall()

    if len(results) > 1:
        print(f"got more than one result for exp_id {exp_id}, ch_id {ch_id}")

    result = results[0]
    if result:
        x_voxel_sizes.append(result[2])
        y_voxel_sizes.append(result[3])
        z_voxel_sizes.append(result[4])
        voxel_units.append(result[5])
        hierarchy_methods.append(result[6])
        downsample_statuses.append(result[7])
        emails.append(result[8])
    else:
        raise ValueError

df["x_voxel_size"] = x_voxel_sizes
df["y_voxel_size"] = y_voxel_sizes
df["z_voxel_size"] = z_voxel_sizes
df["voxel_unit"] = voxel_units
df["hierarchy_method"] = hierarchy_methods
df["downsample_status"] = downsample_statuses
df["creator_email"] = emails

df.to_csv("scripts/public_datasets_downsample.csv", index=False)
