"""Tests for fund_etl.transform.transform."""

import polars as pl
import pytest

from fund_etl.transform.transform import (
    transform_calculations,
    transform_labels,
    transform_numeric_facts,
    transform_submissions,
    transform_tags,
    transform_text_disclosures,
)


def _submissions_df(**overrides):
    data = {
        "adsh": ["0001133228-26-006519"],
        "cik": ["12345"],
        "ein": ["123456789"],
        "nciks": ["1"],
        "name": ["Test Fund"],
        "form": ["N-1A"],
        "filed": ["20260101"],
        "instance": ["test.xml"],
        "pdate": ["20260101"],
        "effdate": ["20260101"],
        "changed": ["20260101"],
        "accepted": ["2026-01-01 10:00:00.000"],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_transform_submissions_basic():
    out = transform_submissions(_submissions_df())
    assert out.shape[0] == 1
    assert out["adsh"][0] == "0001133228-26-006519"
    assert len(out["adsh"][0]) == 20
    assert out["cik"].dtype == pl.Int32
    assert out["ein"].dtype == pl.Int32
    assert out["nciks"].dtype == pl.Int16
    assert out["filed"].dtype == pl.Date
    assert out["pdate"].dtype == pl.Date
    assert out["effdate"].dtype == pl.Date
    assert out["changed"].dtype == pl.Date
    assert out["accepted"].dtype == pl.Datetime


def test_transform_submissions_drops_null_required():
    assert transform_submissions(_submissions_df(name=[None])).is_empty()
    assert transform_submissions(_submissions_df(adsh=[None])).is_empty()


def test_transform_submissions_dedupes_on_adsh():
    df = pl.DataFrame(
        {
            "adsh": ["0001133228-26-000001", "0001133228-26-000001"],
            "cik": ["1", "2"],
            "ein": ["1", "2"],
            "nciks": ["1", "1"],
            "name": ["A", "B"],
            "form": ["N-1A", "N-1A"],
            "filed": ["20260101", "20260102"],
            "instance": ["a.xml", "b.xml"],
            "pdate": ["20260101", "20260101"],
            "effdate": ["20260101", "20260101"],
            "changed": ["20260101", "20260101"],
            "accepted": ["2026-01-01 10:00:00.000", "2026-01-01 11:00:00.000"],
        }
    )
    assert transform_submissions(df).shape[0] == 1


def test_transform_submissions_bad_date_drops_row():
    # strict=False parsing -> null, and filed is in the drop_nulls subset.
    assert transform_submissions(_submissions_df(filed=["not-a-date"])).is_empty()


def _tags_df(**overrides):
    data = {"tag": ["Assets"], "version": ["us-gaap/2025"], "custom": [True], "abstract": [False]}
    data.update(overrides)
    return pl.DataFrame(data)


def test_transform_tags_basic_types():
    out = transform_tags(_tags_df())
    assert out.shape[0] == 1
    assert out["custom"].dtype == pl.Boolean
    assert out["abstract"].dtype == pl.Boolean


def test_transform_tags_fills_null_booleans_with_false():
    df = pl.DataFrame(
        {
            "tag": ["Assets", "Liabilities"],
            "version": ["us-gaap/2025", "us-gaap/2025"],
            "custom": [None, True],
            "abstract": [False, None],
        },
        schema={
            "tag": pl.String,
            "version": pl.String,
            "custom": pl.Boolean,
            "abstract": pl.Boolean,
        },
    )
    out = transform_tags(df)
    assert sorted(out["custom"].to_list()) == [False, True]
    assert sorted(out["abstract"].to_list()) == [False, False]
    assert out["custom"].null_count() == 0
    assert out["abstract"].null_count() == 0


def test_transform_tags_drops_null_tag_or_version():
    df = pl.DataFrame(
        {
            "tag": ["Assets", None],
            "version": ["us-gaap/2025", "us-gaap/2025"],
            "custom": [True, True],
            "abstract": [False, False],
        }
    )
    assert transform_tags(df).shape[0] == 1


def test_transform_tags_dedupes_on_tag_version():
    df = pl.DataFrame(
        {
            "tag": ["Assets", "Assets"],
            "version": ["us-gaap/2025", "us-gaap/2025"],
            "custom": [True, True],
            "abstract": [False, False],
        }
    )
    assert transform_tags(df).shape[0] == 1


def _numeric_df(**overrides):
    data = {
        "adsh": ["0001133228-26-006519"],
        "tag": ["Assets"],
        "version": ["us-gaap/2025"],
        "ddate": ["20260101"],
        "uom": ["USD"],
        "iprx": ["1"],
        "dimn": ["0"],
        "footlen": ["5"],
        "dcml": ["2"],
        "value": ["123.45"],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_transform_numeric_facts_basic_types():
    out = transform_numeric_facts(_numeric_df())
    assert out["ddate"].dtype == pl.Date
    assert out["iprx"].dtype == pl.Int16
    assert out["dimn"].dtype == pl.Int16
    assert out["footlen"].dtype == pl.Int32
    assert out["dcml"].dtype == pl.Int16
    assert out["value"].dtype == pl.Float64
    assert out["value"][0] == pytest.approx(123.45)


def test_transform_numeric_facts_fills_zero_for_null_int_cols():
    df = pl.DataFrame(
        {
            "adsh": ["0001133228-26-006519"],
            "tag": ["Assets"],
            "version": ["us-gaap/2025"],
            "ddate": ["20260101"],
            "uom": ["USD"],
            "iprx": [None],
            "dimn": [None],
            "footlen": [None],
            "dcml": ["2"],
            "value": ["1.0"],
        },
        schema={
            "adsh": pl.String,
            "tag": pl.String,
            "version": pl.String,
            "ddate": pl.String,
            "uom": pl.String,
            "iprx": pl.Int16,
            "dimn": pl.Int16,
            "footlen": pl.Int32,
            "dcml": pl.String,
            "value": pl.String,
        },
    )
    out = transform_numeric_facts(df)
    assert out["iprx"][0] == 0
    assert out["dimn"][0] == 0
    assert out["footlen"][0] == 0


def test_transform_numeric_facts_drops_null_required():
    assert transform_numeric_facts(_numeric_df(adsh=[None])).is_empty()


def test_transform_numeric_facts_invalid_ddate_drops_row():
    assert transform_numeric_facts(_numeric_df(ddate=["bad-date"])).is_empty()



def _text_df(**overrides):
    data = {
        "adsh": ["0001133228-26-006519"],
        "tag": ["Assets"],
        "version": ["us-gaap/2025"],
        "ddate": ["20260101"],
        "iprx": ["1"],
        "dimn": ["0"],
        "dcml": ["2"],
        "escaped": [False],
        "srclen": ["10"],
        "txtlen": ["20"],
        "footlen": ["0"],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_transform_text_disclosures_basic_types():
    out = transform_text_disclosures(_text_df())
    assert out["ddate"].dtype == pl.Date
    assert out["iprx"].dtype == pl.Int16
    assert out["dimn"].dtype == pl.Int16
    assert out["dcml"].dtype == pl.Int32
    assert out["escaped"].dtype == pl.Boolean
    assert out["srclen"].dtype == pl.Int32
    assert out["txtlen"].dtype == pl.Int32
    assert out["footlen"].dtype == pl.Int32


def test_transform_text_disclosures_fills_defaults():
    df = pl.DataFrame(
        {
            "adsh": ["0001133228-26-006519"],
            "tag": ["Assets"],
            "version": ["us-gaap/2025"],
            "ddate": ["20260101"],
            "iprx": [None],
            "dimn": [None],
            "dcml": [None],
            "escaped": [None],
            "srclen": [None],
            "txtlen": [None],
            "footlen": [None],
        },
        schema={
            "adsh": pl.String,
            "tag": pl.String,
            "version": pl.String,
            "ddate": pl.String,
            "iprx": pl.Int16,
            "dimn": pl.Int16,
            "dcml": pl.Int32,
            "escaped": pl.Boolean,
            "srclen": pl.Int32,
            "txtlen": pl.Int32,
            "footlen": pl.Int32,
        },
    )
    out = transform_text_disclosures(df)
    assert out["iprx"][0] == 0
    assert out["dimn"][0] == 0
    assert out["escaped"][0] is False
    assert out["srclen"][0] == 0
    assert out["txtlen"][0] == 0
    assert out["footlen"][0] == 0


def test_transform_text_disclosures_drops_null_required():
    assert transform_text_disclosures(_text_df(tag=[None])).is_empty()


def test_transform_labels_lowercases_columns():
    df = pl.DataFrame(
        {
            "ADSH": ["0001133228-26-006519"],
            "TAG": ["Assets"],
            "VERSION": ["us-gaap/2025"],
            "STD": ["std label"],
        }
    )
    out = transform_labels(df)
    assert out.columns == ["adsh", "tag", "version", "std"]
    assert out.shape[0] == 1


def test_transform_labels_drops_null_required():
    df = pl.DataFrame({"adsh": [None], "tag": ["Assets"], "version": ["us-gaap/2025"]})
    assert transform_labels(df).is_empty()


def test_transform_labels_preserves_already_lowercase():
    df = pl.DataFrame({"adsh": ["x"], "tag": ["t"], "version": ["v"]})
    out = transform_labels(df)
    assert out.columns == ["adsh", "tag", "version"]
    assert out.shape[0] == 1


def _calc_df(**overrides):
    data = {
        "adsh": ["0001133228-26-006519"],
        "grp": ["1"],
        "arc": ["2"],
        "negative": ["1"],
        "ptag": ["Assets"],
        "pversion": ["us-gaap/2025"],
        "ctag": ["Liabilities"],
        "cversion": ["us-gaap/2025"],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_transform_calculations_basic_types():
    out = transform_calculations(_calc_df())
    assert out["grp"].dtype == pl.Int32
    assert out["arc"].dtype == pl.Int32
    assert out["negative"].dtype == pl.Int16


def test_transform_calculations_fills_null_negative():
    df = pl.DataFrame(
        {
            "adsh": ["0001133228-26-006519"],
            "grp": ["1"],
            "arc": ["2"],
            "negative": [None],
            "ptag": ["Assets"],
            "pversion": ["us-gaap/2025"],
            "ctag": ["Liabilities"],
            "cversion": ["us-gaap/2025"],
        },
        schema={
            "adsh": pl.String,
            "grp": pl.String,
            "arc": pl.String,
            "negative": pl.Int16,
            "ptag": pl.String,
            "pversion": pl.String,
            "ctag": pl.String,
            "cversion": pl.String,
        },
    )
    out = transform_calculations(df)
    assert out["negative"][0] == 0


def test_transform_calculations_drops_null_required():
    assert transform_calculations(_calc_df(ptag=[None])).is_empty()

