"""Tests for main.run_pipeline orchestration."""

import polars as pl
import pytest

import main


class _FakeLazy:
    """Minimal stand-in for a polars LazyFrame with a collect() method."""

    def __init__(self, df):
        self._df = df

    def collect(self):
        return self._df


@pytest.fixture
def patched_pipeline(monkeypatch, tmp_path):
    """Patch every external dependency of run_pipeline and record calls."""
    calls = {"bulk_rows": 0, "success": None, "failed": None, "bridge": None, "ticker": 0}

    q_dir = tmp_path / "raw" / "2026q2"
    q_dir.mkdir(parents=True)
    for name in ["sub.tsv", "tag.tsv", "num.tsv", "txt.tsv", "lab.tsv", "cal.tsv"]:
        (q_dir / name).write_text("adsh\tcol\n")

    sample_df = pl.DataFrame({"adsh": ["0001133228-26-006519"], "tag": ["Assets"], "version": ["v"]})

    monkeypatch.setattr(main, "resolve_quarter_id", lambda y, q: 1)
    monkeypatch.setattr(main, "start_pipeline_execution", lambda y, q: 10)
    monkeypatch.setattr(
        main, "download_and_extract_quarter", lambda y, q, force_download=False: q_dir
    )
    monkeypatch.setattr(main, "scan_tsv_lazy", lambda p: _FakeLazy(sample_df))
    monkeypatch.setattr(main, "stream_batches", lambda p, batch_size=1: iter([sample_df]))

    for fn in [
        "transform_submissions",
        "transform_tags",
        "transform_numeric_facts",
        "transform_text_disclosures",
        "transform_labels",
        "transform_calculations",
    ]:
        monkeypatch.setattr(main, fn, lambda df: df)

    def _bulk(df, table, cols, **kwargs):
        calls["bulk_rows"] += df.height
        return df.height

    monkeypatch.setattr(main, "bulk_copy_dataframe", _bulk)

    def _bridge(df, quarter_id):
        calls["bridge"] = quarter_id
        return df.height

    monkeypatch.setattr(main, "link_quarter_taxonomy_tags", _bridge)
    monkeypatch.setattr(main, "sync_mutual_fund_tickers", lambda: calls.__setitem__("ticker", calls["ticker"] + 1))
    monkeypatch.setattr(main, "copy_metadata_and_clean_directory", lambda s, d: None)
    monkeypatch.setattr(
        main,
        "mark_pipeline_success",
        lambda log_id, total: calls.__setitem__("success", (log_id, total)),
    )
    monkeypatch.setattr(
        main,
        "mark_pipeline_failed",
        lambda log_id, err, partial_records=0: calls.__setitem__(
            "failed", (log_id, err, partial_records)
        ),
    )
    return calls


def test_run_pipeline_success_with_ticker(patched_pipeline):
    main.run_pipeline(2026, 2, is_ticker_mf=True)
    assert patched_pipeline["ticker"] == 1
    assert patched_pipeline["failed"] is None
    log_id, total = patched_pipeline["success"]
    assert log_id == 10
    # sub + tag + num + txt + lab + cal -> 6 single-row batches.
    assert total == 6
    assert patched_pipeline["bridge"] == 1


def test_run_pipeline_skips_ticker_when_disabled(patched_pipeline):
    main.run_pipeline(2026, 2, is_ticker_mf=False)
    assert patched_pipeline["ticker"] == 0
    assert patched_pipeline["success"] is not None


def test_run_pipeline_only_required_files(monkeypatch, tmp_path):
    """When only sub/tag/num exist, optional txt/lab/cal steps are skipped."""
    calls = {"tables": []}
    q_dir = tmp_path / "raw" / "2026q2"
    q_dir.mkdir(parents=True)
    for name in ["sub.tsv", "tag.tsv", "num.tsv"]:
        (q_dir / name).write_text("adsh\tcol\n")

    sample_df = pl.DataFrame({"adsh": ["a"], "tag": ["t"], "version": ["v"]})

    monkeypatch.setattr(main, "resolve_quarter_id", lambda y, q: 1)
    monkeypatch.setattr(main, "start_pipeline_execution", lambda y, q: 10)
    monkeypatch.setattr(
        main, "download_and_extract_quarter", lambda y, q, force_download=False: q_dir
    )
    monkeypatch.setattr(main, "scan_tsv_lazy", lambda p: _FakeLazy(sample_df))
    monkeypatch.setattr(main, "stream_batches", lambda p, batch_size=1: iter([sample_df]))
    for fn in ["transform_submissions", "transform_tags", "transform_numeric_facts"]:
        monkeypatch.setattr(main, fn, lambda df: df)

    def _bulk(df, table, cols, **kwargs):
        calls["tables"].append(table)
        return df.height

    monkeypatch.setattr(main, "bulk_copy_dataframe", _bulk)
    monkeypatch.setattr(main, "link_quarter_taxonomy_tags", lambda df, qid: df.height)
    monkeypatch.setattr(main, "sync_mutual_fund_tickers", lambda: None)
    monkeypatch.setattr(main, "copy_metadata_and_clean_directory", lambda s, d: None)
    monkeypatch.setattr(main, "mark_pipeline_success", lambda log_id, total: None)

    main.run_pipeline(2026, 2, is_ticker_mf=False)

    assert "sec_financials.submissions" in calls["tables"]
    assert "sec_financials.taxonomy_tags" in calls["tables"]
    assert "sec_financials.numeric_facts" in calls["tables"]
    assert "sec_financials.text_disclosures" not in calls["tables"]
    assert "sec_financials.presentation_labels" not in calls["tables"]
    assert "sec_financials.calculation_relationships" not in calls["tables"]



def test_run_pipeline_failure_marks_failed_and_reraises(monkeypatch, tmp_path):
    calls = {"failed": None}
    q_dir = tmp_path / "raw" / "2026q2"
    q_dir.mkdir(parents=True)
    (q_dir / "sub.tsv").write_text("adsh\tcol\n")
    (q_dir / "tag.tsv").write_text("adsh\tcol\n")
    (q_dir / "num.tsv").write_text("adsh\tcol\n")

    sample_df = pl.DataFrame({"adsh": ["a"]})

    monkeypatch.setattr(main, "resolve_quarter_id", lambda y, q: 1)
    monkeypatch.setattr(main, "start_pipeline_execution", lambda y, q: 99)
    monkeypatch.setattr(
        main, "download_and_extract_quarter", lambda y, q, force_download=False: q_dir
    )
    monkeypatch.setattr(main, "scan_tsv_lazy", lambda p: _FakeLazy(sample_df))
    monkeypatch.setattr(main, "stream_batches", lambda p, batch_size=1: iter([sample_df]))
    monkeypatch.setattr(main, "transform_submissions", lambda df: df)

    def _boom(*a, **k):
        raise RuntimeError("database is down")

    monkeypatch.setattr(main, "bulk_copy_dataframe", _boom)
    monkeypatch.setattr(
        main,
        "mark_pipeline_failed",
        lambda log_id, err, partial_records=0: calls.__setitem__(
            "failed", (log_id, err, partial_records)
        ),
    )
    monkeypatch.setattr(main, "mark_pipeline_success", lambda log_id, total: None)

    with pytest.raises(RuntimeError, match="database is down"):
        main.run_pipeline(2026, 2, is_ticker_mf=False)

    log_id, err, partial = calls["failed"]
    assert log_id == 99
    assert "database is down" in err
    assert partial == 0

