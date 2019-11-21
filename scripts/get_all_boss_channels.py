"""NOT ONLY PUBLIC"""


import pandas as pd

import pymysql
from sqlalchemy import create_engine

HOST = "localhost"
USER = "ben"
PASSWD = ""
DATABASE = "boss"


db_connection_str = f"mysql+pymysql://{USER}@{HOST}/{DATABASE}"
db_connection = create_engine(db_connection_str)


sql = """select collection.*,experiment.*,channel.*,coordinate_frame.*,auth_user.username
from channel inner join experiment on channel.experiment_id=experiment.id
inner join collection on experiment.collection_id=collection.id
inner join coordinate_frame on experiment.coord_frame_id=coordinate_frame.id
inner join auth_user on channel.creator_id = auth_user.id
where channel.to_be_deleted is NULL order by collection.name,experiment.name,channel.name;
"""

df = pd.read_sql(sql, con=db_connection)

cols = df.columns

swap_cols = [0, 1, 2, 6, 7, 8, 19, 20, 21, 25]
swap_names = [
    "coll_ids",
    "coll",
    "coll_descriptions",
    "exp_ids",
    "exp",
    "exp_description",
    "ch_ids",
    "ch",
    "ch_description",
    "dtype"
]

col_names = [str(c) for c in cols]
for i, n in zip(swap_cols, swap_names):
    col_names[i] = n

df.columns = col_names

df.to_csv("scripts/all_datasets_ids.csv", index=False)
