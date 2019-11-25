from multiprocessing import Pool

import click
import pandas as pd

from boss_export.utils import gen_messages

LAYERPATH = None
OWNER = None


@click.command()
@click.argument("dest_bucket")
@click.option("-p", "--public", is_flag=True, help="Flag for public objects")
@click.option(
    "--iam_role",
    default=None,
    help="IAM role to assume during writes (for cross account writes)",
)
def submit_datasets(dest_bucket, public, iam_role):
    # from dataset_status.py
    df = pd.read_csv("scripts/public_data_sets_to_tx.csv", na_filter=False)

    cmd_args = []
    for _, dataset in df.iterrows():
        cmd_args.append(
            [
                dataset["coll"],
                dataset["exp"],
                dataset["ch"],
                dest_bucket,
                LAYERPATH,
                OWNER,
                public,
                iam_role,
            ]
        )

    with Pool(16) as pool:
        pool.starmap(gen_messages.gen_messages_non_click, cmd_args)


if __name__ == "__main__":
    submit_datasets()  # pylint: disable=no-value-for-parameter
