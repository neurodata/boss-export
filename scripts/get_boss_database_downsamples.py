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

df = pd.read_csv("scripts/public_datasets_scales.csv")

downsamples = []

for _, row in df.iterrows():
    ch_id = row["ch_ids"]

    mycursor.execute(
        f"select channel.name,downsample_status  from channel where id = {ch_id}"
    )

    result = mycursor.fetchall()[0]

    if result:
        downsamples.append(result[1])
    else:
        raise ValueError

df["downsample_status"] = downsamples

df.to_csv("scripts/public_datasets_downsample.csv", index=False)
