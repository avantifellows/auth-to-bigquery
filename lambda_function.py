import json
import os
import logging
from google.cloud import bigquery


def lambda_handler(event):
    messageBody = event["Records"]

    for eachMessage in messageBody:

        # extracting the body of the message
        message = json.loads(eachMessage["Sns"]["Message"])[0]

        row = {}

        # the key values are the column names in the BQ table.
        row["timestamp"] = message["dateTime"]
        row["purpose_type"] = message["purpose"]["type"]
        row["purpose_sub_type"] = message["purpose"]["subType"]
        row["platform"] = message["purpose"]["params"]["platform"]
        row["platform_id"] = message["purpose"]["params"]["id"]
        row["auth_type"] = message["authType"]
        row["user_id"] = message["user"]["values"]
        row["user_data_validated"] = message["user"]["userDataValidated"]

        return insert_data(row)


def insert_data(row):

    # load env variables
    project_id = os.environ.get("BIGQUERY_PROJECT_ID")
    dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
    table_id = os.environ.get("TABLE_ID")

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Inserts data into a table
    errors = client.insert_rows_json(table, [row])
    if errors == []:
        logging.info("New rows have been added")
        return {"statusCode": 200, "body": "All done!"}
    else:
        logging.error(
            "Encountered errors while inserting rows: {}".format(errors))
        return {"statusCode": 500, "body": "Error in adding row!"}
