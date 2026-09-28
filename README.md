# auth-to-bigquery

Writes portal attendance events to BigQuery.

```
portal-backend  --(POST /user-session/send-message)-->  SQS EventQueue
    --> Lambda attendance-to-bigquery (lambda_function.py)
    --> BigQuery avantifellows.auth_logs.attendance-logs
```

portal-backend puts one message per sign-in, sign-up or launch on the queue. The
Lambda turns each message into one row. The row's timestamp is the time SQS
received the message.

- Unknown `type`s and malformed messages are logged and dropped.
- Rows BigQuery rejects, or a whole batch when BigQuery can't be reached, are
  returned as `batchItemFailures`, so SQS retries only those messages. After 10
  receives a message moves to `DeadLetterEventQueue`.
- The SQS message id is the BigQuery insert id, so a retry doesn't duplicate a row.

## Setup

| | |
|---|---|
| Runtime | Python 3.12, x86_64, handler `lambda_function.lambda_handler` |
| Environment | `BIGQUERY_PROJECT_ID=avantifellows`, `BIGQUERY_DATASET_ID=auth_logs`, `TABLE_ID=attendance-logs`, `GCP_CREDENTIALS_SECRET=<secret name>` |
| Credentials | GCP service account JSON stored in Secrets Manager (never in the zip) |
| IAM role | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` on the queue; `secretsmanager:GetSecretValue` on the secret; CloudWatch logs |
| Trigger | SQS event source mapping on the queue, batch size 10, **Report batch item failures** turned on |

The queue's visibility timeout must be longer than the function timeout.

## Deploy

Needs [uv](https://docs.astral.sh/uv/) and the AWS CLI.

```sh
./deploy.sh attendance-to-bigquery
```

Staging uses the same code with its own function, queue (`stagingEventQueue`) and
`TABLE_ID`.

## Tests

```sh
pip install pytest boto3
PYTHONPATH=. pytest tests
```

## Pre-commit

flake8 (max line length 88) and autopep8 run on commit:

```sh
pip install pre-commit && pre-commit install
```
