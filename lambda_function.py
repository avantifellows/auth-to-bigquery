"""Portal attendance events: SQS queue -> BigQuery.

portal-backend puts one message per login or launch on the queue; each becomes a
row in auth_logs.attendance-logs. Messages that fail to insert are reported back
to SQS so only those are retried; the SQS message id doubles as the BigQuery
insert id, so a retry cannot create a duplicate row.
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ACCEPTED_TYPES = {
    "attendance",
    "attendance-on-sign-up",
    "broadcast",
    "popup_form",
    "sign-in",
    "sign-up",
}

# message field -> column
COLUMNS = {
    "type": "purpose_type",
    "sub_type": "purpose_subtype",
    "platform": "platform",
    "platform_id": "platform_id",
    "auth_type": "auth_type",
    "user_id": "user_id",
    "user_validated": "user_data_validated",
    "user_type": "userType",
    "auth_group": "group",
    "session_id": "session_id",
    "user_ip_address": "user_ip_address",
    "phone_number": "phone_number",
    "batch": "batch",
    "date_of_birth": "date_of_birth",
}

_client = None


def bigquery_client():
    global _client
    if _client is None:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        secret = boto3.client("secretsmanager").get_secret_value(
            SecretId=os.environ["GCP_CREDENTIALS_SECRET"]
        )
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(secret["SecretString"])
        )
        _client = bigquery.Client(
            project=os.environ["BIGQUERY_PROJECT_ID"], credentials=credentials
        )
    return _client


def to_row(record):
    """The BigQuery row for one SQS record, or None if the message is unusable."""
    try:
        body = json.loads(record["body"])
    except ValueError:
        return None
    message = body[0] if isinstance(body, list) and body else body
    if not isinstance(message, dict) or message.get("type") not in ACCEPTED_TYPES:
        return None

    sent_ms = int(record["attributes"]["SentTimestamp"])
    row = {
        column: message.get(field, "") for field, column in COLUMNS.items()
    }
    row["user_data_validated"] = bool(message.get("user_validated", True))
    row["attendance_timestamp"] = datetime.fromtimestamp(
        sent_ms / 1000, timezone.utc
    ).isoformat()
    row["number_of_multiple_entries"] = "1"
    return row


def lambda_handler(event, context):
    ids, rows = [], []
    for record in event.get("Records", []):
        row = to_row(record)
        if row is None:
            # Retrying cannot fix a malformed or unknown message, so drop it.
            logger.warning("Skipping message %s: %s", record.get("messageId"),
                           record.get("body"))
            continue
        ids.append(record["messageId"])
        rows.append(row)

    failed = []
    if rows:
        table = "{}.{}.{}".format(
            os.environ["BIGQUERY_PROJECT_ID"],
            os.environ["BIGQUERY_DATASET_ID"],
            os.environ["TABLE_ID"],
        )
        try:
            errors = bigquery_client().insert_rows_json(table, rows, row_ids=ids)
            failed = [ids[error["index"]] for error in errors]
            if errors:
                logger.error("Insert errors: %s", errors)
        except Exception:
            logger.exception("Insert failed for %d rows", len(rows))
            failed = ids

    logger.info("Inserted %d of %d messages", len(rows) - len(failed),
                len(event.get("Records", [])))
    return {"batchItemFailures": [{"itemIdentifier": i} for i in failed]}
