import json
import os
import logging
from google.cloud import bigquery

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, lambda_context):
    """
    Parses messages sent to liveclassAttendanceEventHandler lambda function.
    Each message needs to have the following fields for row to be inserted:
        - dateTime
        - purpose
            - type
            - subType
            - params
                - platform
                - id
        - authType
        - user
            - values
    When multiple user IDs are entered,
    each userID and its valid flag is inserted as a seperate row in BigQuery.
    """
    messageBody = event["Records"]

    for eachMessage in messageBody:
        if "Sns" in eachMessage and "Message" in eachMessage["Sns"]:
            # extracting the body of the message
            message = json.loads(eachMessage["Sns"]["Message"])[0]

            # contains all the keys (or fields) and their values to be added into BigQuery.
            row = {}

            # check if the key values exist in the message sent
            # even if one field is missing,
            #   the row isn't inserted and an error is logged

            if all(
                (k in message for k in ("dateTime", "purpose", "authType", "user"))
                and (k in message["purpose"] for k in ("type", "subType", "params"))
                and (k in message["user"] for k in ("values"))
                and (k in message["purpose"]["params"] for k in ("platform", "id"))
            ):
                # parsing through each user ID and its respective valid flag
                for each in message["user"]["values"]:
                    # the key values are the column names in the BQ table.
                    row["timestamp"] = message["dateTime"]
                    row["purpose_type"] = message["purpose"]["type"]
                    row["purpose_sub_type"] = message["purpose"]["subType"]
                    row["platform"] = message["purpose"]["params"]["platform"]
                    row["platform_id"] = message["purpose"]["params"]["id"]
                    row["auth_type"] = message["authType"]
                    row["user_id"] = each["userID"]
                    row["user_data_validated"] = each["valid"]

                    return insert_data(row)

            else:
                logger.info(
                    "Encountered missing fields in message: {}".format(message))

        else:
            logger.info(
                "Encountered missing fields in message: {}".format(eachMessage))


def insert_data(row):
    """
    Function which inserts row into bigquery.
    Project ID, Dataset ID, Table ID ae all stored as .env variables.
    """

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
        logging.info("New row has been added")
        logging.info(row)
        return {"statusCode": 200, "body": "All done!"}

    else:
        logging.error(
            "Encountered errors while inserting row: {}".format(errors))
        logging.error(row)

        return {"statusCode": 500, "body": "Error in adding row!"}
