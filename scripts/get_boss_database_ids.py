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

df = pd.read_csv("scripts/public_datasets.csv")

coll_ids = []
exp_ids = []
ch_ids = []

for index, row in df.iterrows():
    coll = row["coll"]
    exp = row["exp"]
    ch = row["ch"]

    mycursor.execute(
        f"select * from lookup where collection_name='{coll}' and experiment_name='{exp}' and channel_name='{ch}'"
    )

    for x in mycursor:
        X = x[1].split("&")
        coll_ids.append(int(X[0]))
        exp_ids.append(int(X[1]))
        ch_ids.append(int(X[2]))

df["coll_ids"] = coll_ids
df["exp_ids"] = exp_ids
df["ch_ids"] = ch_ids

df.to_csv("public_datasets_ids.csv")
