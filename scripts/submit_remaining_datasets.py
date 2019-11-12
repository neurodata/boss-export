from multiprocessing import Pool

import pandas as pd

from boss_export.utils import gen_messages

DEST_BUCKET = "open-neurodata"


# from dataset_status.py
df = pd.read_csv("scripts/public_data_sets_to_tx.csv", na_filter=False)

# gen_messages(coll, exp, ch, dest_bucket, None)
cmd_args = []
for _, dataset in df.iterrows():
    cmd_args.append([dataset["coll"], dataset["exp"], dataset["ch"], DEST_BUCKET])


with Pool(8) as pool:
    pool.starmap(gen_messages.gen_messages_non_click, cmd_args)
