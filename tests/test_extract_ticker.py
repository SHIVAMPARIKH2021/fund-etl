"""Tests for fund_etl.extract.extract_ticker."""

import httpx
import pytest

from fund_etl.extract.extract_ticker import sync_mutual_fund_tickers
from tests.conftest import FakeGetResponse

SAMPLE_PAYLOAD = {
    "data": [
        [123456, "S000001", "C000001", "AAAAX"],
        [123456, "S000001", "C000002", "AAAAY"],
        [789012, "S000002", "C000003", "BBBBX"],
    ]
}


def test_sync_tickers_success(db, fake_get_client):
    db.cursor.rowcount = 0
    fake_get_client(FakeGetResponse(payload=SAMPLE_PAYLOAD))
    sync_mutual_fund_tickers()

    assert db.conn.commits == 1
    # temp table create, insert, and update statements all executed.
    executed_sql = [q for q, _ in db.cursor.executed]
    assert any("CREATE TEMP TABLE" in q for q in executed_sql)
    assert any("INSERT INTO" in q for q in executed_sql)
    assert any("UPDATE" in q for q in executed_sql)
    # COPY payload was streamed.
    assert len(db.cursor.copied_chunks) > 0
    # The UPDATE carries the sync_start watermark.
    _, update_params = db.cursor.executed[-1]
    assert "sync_start" in update_params


def test_sync_tickers_logs_deactivations(db, fake_get_client):
    db.cursor.rowcount = 5
    fake_get_client(FakeGetResponse(payload=SAMPLE_PAYLOAD))
    # Should not raise when defunct rows are detected.
    sync_mutual_fund_tickers()
    assert db.conn.commits == 1


def test_sync_tickers_http_error_propagates(db, fake_get_client):
    err = httpx.HTTPStatusError(
        "boom", request=httpx.Request("GET", "http://example.com"), response=None
    )
    fake_get_client(FakeGetResponse(raise_exc=err))
    with pytest.raises(httpx.HTTPStatusError):
        sync_mutual_fund_tickers()
    assert db.conn.commits == 0


def test_sync_tickers_copies_tab_separated_rows(db, fake_get_client):
    db.cursor.rowcount = 0
    fake_get_client(FakeGetResponse(payload=SAMPLE_PAYLOAD))
    sync_mutual_fund_tickers()
    payload = b"".join(db.cursor.copied_chunks).decode()
    # Three share classes were serialized, tab separated, no header.
    lines = [ln for ln in payload.splitlines() if ln.strip()]
    assert len(lines) == 3
    assert all("\t" in ln for ln in lines)
    assert "AAAAX" in payload


def test_sync_tickers_empty_payload(db, fake_get_client):
    db.cursor.rowcount = 0
    fake_get_client(FakeGetResponse(payload={"data": []}))
    sync_mutual_fund_tickers()
    # No rows means nothing to COPY, but temp table + statements still run.
    assert db.conn.commits == 1
    assert db.cursor.copied_chunks == []
