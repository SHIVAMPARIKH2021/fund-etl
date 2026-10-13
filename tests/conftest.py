"""Shared pytest fixtures and lightweight fakes for the fund-etl test suite.

These fakes stand in for the two external boundaries the pipeline touches:

* ``psycopg`` database connections (context-manager based).
* ``httpx`` HTTP clients (context-manager based).

Using fakes keeps the unit tests fast, deterministic and free of any real
network or database dependency.
"""

from types import SimpleNamespace

import httpx
import psycopg
import pytest


# ---------------------------------------------------------------------------
# psycopg fakes
# ---------------------------------------------------------------------------
class FakeCopy:
    """Context manager returned by ``cursor.copy(...)`` capturing written bytes."""

    def __init__(self, sink: list):
        self._sink = sink

    def __enter__(self) -> "FakeCopy":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def write(self, chunk) -> None:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        self._sink.append(chunk)


class FakeCursor:
    """Records executed statements and returns scripted ``fetchone`` results."""

    def __init__(self, fetchone_result=(1,), rowcount: int = 0):
        self._fetchone_result = fetchone_result
        self.rowcount = rowcount
        self.executed: list = []
        self.copied_chunks: list = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, query, params=None) -> None:
        self.executed.append((str(query), params))

    def executemany(self, query, seq) -> None:
        self.executed.append((str(query), list(seq)))

    def fetchone(self):
        return self._fetchone_result

    def copy(self, statement):
        return FakeCopy(self.copied_chunks)


class FakeConnection:
    """Context-manager connection exposing a single shared cursor."""

    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


@pytest.fixture
def db(monkeypatch):
    """Patch ``psycopg.connect`` and expose the fake connection/cursor.

    Tests can tune behaviour before invoking the code under test, e.g.::

        db.cursor._fetchone_result = (42,)
        db.cursor.rowcount = 3
    """
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    def _connect(*_args, **_kwargs):
        return conn

    monkeypatch.setattr(psycopg, "connect", _connect)
    return SimpleNamespace(conn=conn, cursor=cursor)


# ---------------------------------------------------------------------------
# httpx fakes
# ---------------------------------------------------------------------------
class FakeStreamResponse:
    """Response returned by ``client.stream(...)`` for the extract download."""

    def __init__(self, status_code: int = 200, chunks=(b"data",), raise_exc=None):
        self.status_code = status_code
        self._chunks = list(chunks)
        self._raise_exc = raise_exc

    def __enter__(self) -> "FakeStreamResponse":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def raise_for_status(self) -> None:
        if self._raise_exc is not None:
            raise self._raise_exc

    def iter_bytes(self, chunk_size: int = 1):
        yield from self._chunks


class FakeStreamClient:
    """Client whose ``stream`` returns a scripted :class:`FakeStreamResponse`."""

    def __init__(self, response: FakeStreamResponse):
        self._response = response
        self.stream_called = False

    def __enter__(self) -> "FakeStreamClient":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def stream(self, method, url):
        self.stream_called = True
        return self._response


class FakeGetResponse:
    """Response returned by ``client.get(...)`` for the ticker sync."""

    def __init__(self, payload=None, raise_exc=None):
        self._payload = payload if payload is not None else {}
        self._raise_exc = raise_exc

    def raise_for_status(self) -> None:
        if self._raise_exc is not None:
            raise self._raise_exc

    def json(self):
        return self._payload


class FakeGetClient:
    """Client whose ``get`` returns a scripted :class:`FakeGetResponse`."""

    def __init__(self, response: FakeGetResponse):
        self._response = response
        self.get_called = False

    def __enter__(self) -> "FakeGetClient":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def get(self, url):
        self.get_called = True
        return self._response


@pytest.fixture
def fake_stream_client(monkeypatch):
    """Install a fake ``httpx.Client`` exposing a ``stream`` based response."""

    def factory(response: FakeStreamResponse) -> FakeStreamClient:
        client = FakeStreamClient(response)
        monkeypatch.setattr(httpx, "Client", lambda *_a, **_k: client)
        return client

    return factory


@pytest.fixture
def fake_get_client(monkeypatch):
    """Install a fake ``httpx.Client`` exposing a ``get`` based response."""

    def factory(response: FakeGetResponse) -> FakeGetClient:
        client = FakeGetClient(response)
        monkeypatch.setattr(httpx, "Client", lambda *_a, **_k: client)
        return client

    return factory
