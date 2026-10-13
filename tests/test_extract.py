"""Tests for fund_etl.extract.extract."""

import io
import zipfile

import httpx
import polars as pl
import pytest

from fund_etl.extract import initialize_extraction
from fund_etl.extract.extract import (
    SEC_DATASET_FILES,
    download_and_extract_quarter,
    scan_tsv_lazy,
    stream_batches,
)
from tests.conftest import FakeStreamResponse


def test_initialize_extraction_prints(capsys):
    initialize_extraction()
    captured = capsys.readouterr()
    assert "Initialized data extraction" in captured.out



def _make_zip_bytes(names):
    """Return bytes of an in-memory zip containing the given members."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in names:
            zf.writestr(name, f"content-of-{name}\n")
    return buf.getvalue()


def _seed_destination(target_base_dir, quarter_label):
    """Create a destination dir already containing all expected dataset files."""
    dest = target_base_dir / quarter_label
    dest.mkdir(parents=True, exist_ok=True)
    for f in SEC_DATASET_FILES:
        (dest / f).write_text("existing\n")
    return dest


# ---------------------------------------------------------------------------
# download_and_extract_quarter
# ---------------------------------------------------------------------------
def test_download_skips_when_files_already_exist(fake_stream_client, tmp_path):
    _seed_destination(tmp_path, "2026q2")
    client = fake_stream_client(FakeStreamResponse())
    result = download_and_extract_quarter(2026, 2, target_base_dir=tmp_path)
    assert result == tmp_path / "2026q2"
    assert client.stream_called is False


def test_download_reruns_when_force_true(fake_stream_client, tmp_path):
    _seed_destination(tmp_path, "2026q2")
    zip_bytes = _make_zip_bytes(SEC_DATASET_FILES)
    client = fake_stream_client(FakeStreamResponse(status_code=200, chunks=[zip_bytes]))
    result = download_and_extract_quarter(2026, 2, target_base_dir=tmp_path, force_download=True)
    assert client.stream_called is True
    assert result == tmp_path / "2026q2"
    for f in SEC_DATASET_FILES:
        assert (result / f).exists()
    assert not (result / "2026q2.zip").exists()


def test_download_success_extracts_and_cleans_up(fake_stream_client, tmp_path):
    zip_bytes = _make_zip_bytes(SEC_DATASET_FILES)
    fake_stream_client(
        FakeStreamResponse(status_code=200, chunks=[zip_bytes[:10], zip_bytes[10:]])
    )
    result = download_and_extract_quarter(2026, 2, target_base_dir=tmp_path)
    assert result == tmp_path / "2026q2"
    for f in SEC_DATASET_FILES:
        assert (result / f).exists()
    assert not (result / "2026q2.zip").exists()


def test_download_404_raises_file_not_found(fake_stream_client, tmp_path):
    fake_stream_client(FakeStreamResponse(status_code=404))
    with pytest.raises(FileNotFoundError):
        download_and_extract_quarter(2026, 2, target_base_dir=tmp_path)


def test_download_http_error_propagates(fake_stream_client, tmp_path):
    err = httpx.HTTPStatusError(
        "server error", request=httpx.Request("GET", "http://example.com"), response=None
    )
    fake_stream_client(FakeStreamResponse(status_code=500, raise_exc=err))
    with pytest.raises(httpx.HTTPStatusError):
        download_and_extract_quarter(2026, 2, target_base_dir=tmp_path)


def test_download_rejects_zip_path_traversal(fake_stream_client, tmp_path):
    zip_bytes = _make_zip_bytes(["../evil.txt"])
    fake_stream_client(FakeStreamResponse(status_code=200, chunks=[zip_bytes]))
    with pytest.raises(RuntimeError):
        download_and_extract_quarter(2026, 2, target_base_dir=tmp_path)


# ---------------------------------------------------------------------------
# scan_tsv_lazy
# ---------------------------------------------------------------------------
def test_scan_tsv_lazy_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        scan_tsv_lazy(tmp_path / "does-not-exist.tsv")


def test_scan_tsv_lazy_returns_lazyframe(tmp_path):
    tsv = tmp_path / "sub.tsv"
    tsv.write_text("adsh\tnum\n0001\t1\n0002\t2\n")
    lf = scan_tsv_lazy(tsv)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().shape == (2, 2)


def test_scan_tsv_lazy_treats_empty_as_null(tmp_path):
    tsv = tmp_path / "sub.tsv"
    tsv.write_text("adsh\tnum\n0001\t\n")
    collected = scan_tsv_lazy(tsv).collect()
    assert collected["num"][0] is None


# ---------------------------------------------------------------------------
# stream_batches
# ---------------------------------------------------------------------------
def test_stream_batches_yields_dataframe_slices(tmp_path):
    tsv = tmp_path / "num.tsv"
    tsv.write_text("a\tb\n" + "\n".join(f"{i}\t{i}" for i in range(5)) + "\n")
    batches = list(stream_batches(tsv, batch_size=2))
    assert len(batches) == 3  # 2 + 2 + 1
    assert all(isinstance(b, pl.DataFrame) for b in batches)
    assert sum(b.height for b in batches) == 5


def test_stream_batches_single_batch_when_small(tmp_path):
    tsv = tmp_path / "num.tsv"
    tsv.write_text("a\tb\n1\t2\n")
    batches = list(stream_batches(tsv, batch_size=1000))
    assert len(batches) == 1
    assert batches[0].height == 1


def test_stream_batches_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(stream_batches(tmp_path / "missing.tsv"))

