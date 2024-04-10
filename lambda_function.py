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
        - date_time
        - type
        - sub_type
        - platform
        - platform_id
        - auth_type
        - user_id
        - user_validated
        - auth_group
        - user_type
        - session_id
        - user_ip_address
        - phone_number
        - batch
        - date_of_birth
    """
    messageBody = event["Records"]

    for eachMessage in messageBody:
        if "Sns" in eachMessage and "Message" in eachMessage["Sns"]:
            message = json.loads(eachMessage["Sns"]["Message"])[0]

            # check if the key values exist in the message sent
            # even if one field is missing,
            #   the row isn't inserted and an error is logged
            if all(
                (k in message for k in ("date_time", "type", "sub_type", "platform", "platform_id", "auth_type", "user_id",
                 "user_validated", "auth_group", "user_type", "session_id", "user_ip_address", "phone_number", "batch", "date_of_birth"))
            ):

                row = {}

                row["attendance_timestamp"] = message["date_time"]
                row["purpose_type"] = message["type"]
                row["purpose_subtype"] = message["sub_type"]
                row["platform"] = message["platform"]
                row["platform_id"] = message["platform_id"]
                row["auth_type"] = message["auth_type"]
                row["user_id"] = message["user_id"]
                row["user_data_validated"] = message["user_validated"]
                row["number_of_multiple_entries"] = 1
                row["userType"] = message["user_type"]
                row["group"] = message["auth_group"]
                row["session_id"] = message["session_id"]
                row["user_ip_address"] = message["user_ip_address"]
                row["phone_number"] = message["phone_number"]
                row["batch"] = message["batch"]
                row["date_of_birth"] = message["date_of_birth"]

                print(row)
                insert_data(row)
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

    if row["purpose_subtype"] == "incorrect-entry":
        table_id = os.environ.get("INCORRECT_ENTRY_TABLE_ID")

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

        return {"statusCode": 500, "body": "Error in adding rows!"}
