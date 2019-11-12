#%%
import asyncio
from datetime import datetime

import aiofiles
import aiohttp
import pandas as pd

OPEN_DATA_BUCKET_URL = "https://open-neurodata.s3.amazonaws.com"

#%%
# read the data
df = pd.read_csv("scripts/public_datasets_downsample.csv", na_filter=False)
# removing empty/dev channels in boss
df = df[(df["ch"] != "empty") & (df["ch"] != "dev")]

# we transferred these into a different prefix
df = df[(df["coll"] != "bock") & (df["exp"] != "kasthuri14s1colANNO")]


#%%
def return_url_dataset(coll, exp, ch):
    return f"{OPEN_DATA_BUCKET_URL}/{coll}/{exp}/{ch}/info"



# %%
df["url"] = df.apply(
    lambda x: return_url_dataset(x["coll"], x["exp"], x["ch"]), axis=1
)


# %%
outfname = "scripts/datasets_status_async.csv"
header = df.to_csv(header=None)
with open(outfname, mode='w') as f:
    f.write(",".join(df.columns.to_list()) + ",status_code" + "\n")

async with aiohttp.ClientSession() as session:
    async with aiofiles.open(outfname, mode='a') as f:
        for _, dataset in df.iterrows():
            async with session.get(dataset["url"]) as resp:
                await resp.text()
                data = ",".join(map(str, dataset.to_list())) + "," + str(resp.status) + "\n"
                await f.write(data)

#%%
df_to_do = pd.read_csv(outfname, na_filter=False)
df_to_do = df_to_do[df_to_do["status_code"] != 200]

df_to_do.to_csv("scripts/public_data_sets_to_tx.csv", index=False)

# %%
