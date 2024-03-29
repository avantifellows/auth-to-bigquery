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
        - program
    When multiple user IDs are entered,
    each row is created with a separate user ID's information and added to an array. This array, containing different rows, is added to the table.
    """
    messageBody = event["Records"]

    for eachMessage in messageBody:
        if "Sns" in eachMessage and "Message" in eachMessage["Sns"]:
            message = json.loads(eachMessage["Sns"]["Message"])[0]

            # check if the key values exist in the message sent
            # even if one field is missing,
            #   the row isn't inserted and an error is logged
            if all(
                (k in message for k in ("dateTime", "purpose", "authType",
                 "user", "program", "userType", "sessionId", "userData"))
                and (k in message["purpose"] for k in ("type", "subType", "params"))
                and (k in message["user"] for k in ("values"))
                and (k in message["purpose"]["params"] for k in ("platform", "id"))

            ):

                user_values = message["user"]["values"]
                user_values_length = len(user_values)
                print(message)
                for i in range(user_values_length):
                    row = {}

                    row["attendance_timestamp"] = message["dateTime"]
                    row["purpose_type"] = message["purpose"]["type"]
                    row["purpose_subtype"] = message["purpose"]["subType"]
                    row["platform"] = message["purpose"]["params"]["platform"]
                    row["platform_id"] = message["purpose"]["params"]["id"]
                    row["auth_type"] = message["authType"]
                    row["user_id"] = user_values[i]["userID"]
                    row["user_data_validated"] = user_values[i]["valid"]
                    row["number_multiple_entries"] = user_values_length
                    row["program"] = message["group"]
                    row["userType"] = message["userType"]
                    row["session_id"] = message["sessionId"]
                    row["user_ip_address"] = message["userNetworkData"]["userIp"]
                    print(i, row)
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
