import pytest
import polars as pl
from fund_etl.transform.transform import transform_submissions

def test_transform_submissions_basic():
    # Create a minimal sample dataframe
    data = {
        "adsh": ["0001133228-26-006519"],
        "cik": ["12345"],
        "name": ["Test Fund"],
        "form": ["N-1A"],
        "filed": ["20260101"],
        "instance": ["test.xml"],
        "nciks": ["1"],
        "ein": ["123456789"],
        "pdate": ["20260101"],
        "effdate": ["20260101"],
        "changed": ["20260101"],
        "accepted": ["2026-01-01 10:00:00.000"]
    }
    df = pl.DataFrame(data)
    
    transformed_df = transform_submissions(df)
    
    assert transformed_df.shape[0] == 1
    assert transformed_df["adsh"][0] == "0001133228-26-006519"
    assert len(transformed_df["adsh"][0]) == 20
    assert transformed_df["cik"].dtype == pl.Int32
    assert transformed_df["filed"].dtype == pl.Date

