import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account

def lambda_handler(event, context):
    messageBody = event["Records"]

    for eachMessage in messageBody:
        message = json.loads(eachMessage["Sns"]["Message"])[0]
        
        row = {}
        
        row["DateTime"] = message["DateTime"]
        row["Purpose_type"] = message["Purpose"]["type"]
        row["Purpose_subtype"] = message["Purpose"]["subtype"]
        row["Platform"] = message["Purpose"]["params"]["platform"] 
        row["Resource_ID"] = message["Purpose"]["params"]["id"] 
        row["Auth_type"] = message["auth_type"] 
        row["UserID"] = message["User"]["values"] 
        row["User_validated"] = message["User"]["userdata_validated"]

        insert_data(row)
        
        return {
            'statusCode': 200,
            "body": message,
        }

def insert_data(row):
    """The main handler function. """
    # load env variables
    project_id = os.environ.get('BIGQUERY_PROJECT_ID')
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    tableId = os.environ.get('TABLE_ID')

    client = bigquery.Client(project=project_id)
    bigquery_client = client.dataset(dataset_id).table(tableId)

    #Inserts data into a table
    table = client.get_table(bigquery_client)
    print(row)

    errors = client.insert_rows_json(table, [row])
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    return {"statusCode": 200, "body": "All done!"}

