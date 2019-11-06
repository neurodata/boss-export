#%%
import datetime
import time


import boto3

#%%
SESSION = boto3.Session(profile_name="ben-boss-dev")
SQS = SESSION.resource("sqs", region_name="us-east-1")

SQS_NAME = "copy-boss-cuboids"
queue = SQS.get_queue_by_name(QueueName=SQS_NAME)

DEADLETTER_SQS_NAME = "copy-boss-cuboids-deadletter"
deadletter_queue = SQS.get_queue_by_name(QueueName=DEADLETTER_SQS_NAME)


#%%
now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
with open(
    f"/home/ben/repos/boss-export/scripts/logs/deadletter_{now}.log", "a"
) as deadletterlogfile:
    ntries = 0
    while True:
        msg_count = 0
        entries, delete_entries = [], []
        for message in deadletter_queue.receive_messages(MaxNumberOfMessages=10):
            msg = message.body
            deadletterlogfile.write(msg + "\n")

            Id = message.message_id
            entry = {"Id": Id, "MessageBody": msg}
            entries.append(entry)

            delete_entries.append(
                {"Id": message.message_id, "ReceiptHandle": message.receipt_handle}
            )

            msg_count += 1

        if msg_count == 0:
            if ntries > 2:
                break
            else:
                print("got no messages, waiting 2 secs before trying again...")
                time.sleep(2)
                ntries += 1
        else:
            queue.send_messages(Entries=entries)

            deadletter_queue.delete_messages(Entries=delete_entries)


# %%
