import base64
import functions_framework
from google.cloud import bigquery
import json

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    message_published=base64.b64decode(cloud_event.data["message"]["data"]).decode("utf8")

    data=json.loads(message_published)
    project_id="aerobic-datum-424113-v0"
    dataset_id="project_dataset"
    table_id="logistic_orders_tracking"

#  create bigquery client
    client = bigquery.Client(project_id)

    try:
    # defining table and dataset
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

    # row to insert
        row = [data]
        insert_row=client.insert_rows_json(table,row)

        print(f"Record inserted : {insert_row}")

    except Exception as err:
        print(f"Failed to insert record due to error: {err}")

