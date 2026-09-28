import json

import pytest

import lambda_function as handler


def record(message_id, message, sent_ms=1790000000000):
    return {
        "messageId": message_id,
        "body": json.dumps([message]),
        "attributes": {"SentTimestamp": str(sent_ms)},
    }


def message(**overrides):
    base = {
        "type": "sign-in",
        "sub_type": "gurukul-reports",
        "platform": "report",
        "platform_id": "EnableStudents_S1",
        "auth_type": "ID,DOB",
        "user_id": "504059",
        "auth_group": "EnableStudents",
        "user_type": "student",
        "session_id": "",
        "user_ip_address": "",
        "phone_number": "",
        "batch": "",
        "date_of_birth": "",
        "user_validated": True,
    }
    base.update(overrides)
    return base


class FakeClient:
    def __init__(self, errors=None, raises=False):
        self.calls = []
        self.errors = errors or []
        self.raises = raises

    def insert_rows_json(self, table, rows, row_ids):
        self.calls.append((table, rows, row_ids))
        if self.raises:
            raise RuntimeError("bigquery down")
        return self.errors


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("BIGQUERY_PROJECT_ID", "p")
    monkeypatch.setenv("BIGQUERY_DATASET_ID", "d")
    monkeypatch.setenv("TABLE_ID", "t")


def run(records, client):
    handler._client = client
    return handler.lambda_handler({"Records": records}, None)


def test_every_message_in_a_batch_is_inserted():
    client = FakeClient()
    result = run([record(f"m{i}", message(user_id=str(i))) for i in range(10)], client)

    table, rows, ids = client.calls[0]
    assert table == "p.d.t"
    assert [r["user_id"] for r in rows] == [str(i) for i in range(10)]
    assert ids == [f"m{i}" for i in range(10)]
    assert result == {"batchItemFailures": []}


def test_row_matches_the_table_columns():
    client = FakeClient()
    run([record("m1", message())], client)

    row = client.calls[0][1][0]
    assert row["purpose_type"] == "sign-in"
    assert row["purpose_subtype"] == "gurukul-reports"
    assert row["group"] == "EnableStudents"
    assert row["userType"] == "student"
    assert row["user_data_validated"] is True
    assert row["number_of_multiple_entries"] == "1"
    assert row["attendance_timestamp"] == "2026-09-21T14:13:20+00:00"


def test_unknown_types_and_malformed_bodies_are_dropped_not_retried():
    client = FakeClient()
    bad_body = {"messageId": "m3", "body": "x", "attributes": {"SentTimestamp": "1"}}
    records = [record("m1", message(type="error")), record("m2", message()), bad_body]
    result = run(records, client)

    assert client.calls[0][2] == ["m2"]
    assert result == {"batchItemFailures": []}


def test_only_rows_bigquery_rejects_are_retried():
    client = FakeClient(errors=[{"index": 1, "errors": ["bad"]}])
    result = run([record("m1", message()), record("m2", message())], client)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m2"}]}


def test_the_whole_batch_is_retried_when_bigquery_is_unreachable():
    records = [record("m1", message()), record("m2", message())]
    result = run(records, FakeClient(raises=True))

    failed = [item["itemIdentifier"] for item in result["batchItemFailures"]]
    assert failed == ["m1", "m2"]
