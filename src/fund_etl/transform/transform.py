import polars as pl


def transform_submissions(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans sub.tsv for funds.submissions."""
    return (
        df.with_columns(
            pl.col("cik").cast(pl.Int32, strict=False),
            pl.col("ein").cast(pl.Int32, strict=False),
            pl.col("nciks").cast(pl.Int16, strict=False),
            # Cast to String first to handle both i64 and String inferred columns
            pl.col("pdate").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("effdate").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("filed").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("changed").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("accepted").cast(pl.String).str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
        )
        .drop_nulls(subset=["adsh", "cik", "name", "form", "filed", "instance", "nciks"])
        .unique(subset=["adsh"])
    )


def transform_tags(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans tag.tsv for funds.taxonomy_tags."""
    return (
        df.with_columns(
            pl.col("custom").cast(pl.Boolean, strict=False).fill_null(False),
            pl.col("abstract").cast(pl.Boolean, strict=False).fill_null(False),
        )
        .drop_nulls(subset=["tag", "version"])
        .unique(subset=["tag", "version"])
    )


def transform_numeric_facts(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans num.tsv for funds.numeric_facts."""
    return (
        df.with_columns(
            pl.col("ddate").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("iprx").cast(pl.Int16, strict=False).fill_null(0),
            pl.col("dimn").cast(pl.Int16, strict=False).fill_null(0),
            pl.col("footlen").cast(pl.Int32, strict=False).fill_null(0),
            pl.col("dcml").cast(pl.Int16, strict=False),
            pl.col("value").cast(pl.Float64, strict=False),
        )
        .drop_nulls(subset=["adsh", "tag", "version", "ddate", "uom", "iprx"])
    )


def transform_text_disclosures(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans txt.tsv for funds.text_disclosures."""
    return (
        df.with_columns(
            pl.col("ddate").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            pl.col("iprx").cast(pl.Int16, strict=False).fill_null(0),
            pl.col("dimn").cast(pl.Int16, strict=False).fill_null(0),
            pl.col("dcml").cast(pl.Int32, strict=False),
            pl.col("escaped").cast(pl.Boolean, strict=False).fill_null(False),
            pl.col("srclen").cast(pl.Int32, strict=False).fill_null(0),
            pl.col("txtlen").cast(pl.Int32, strict=False).fill_null(0),
            pl.col("footlen").cast(pl.Int32, strict=False).fill_null(0),
        )
        .drop_nulls(subset=["adsh", "tag", "version", "ddate", "iprx"])
    )


def transform_labels(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans lab.tsv for funds.presentation_labels."""
    cols = {c: c.lower() for c in df.columns}
    return (
        df.rename(cols)
        .drop_nulls(subset=["adsh", "tag", "version"])
    )


def transform_calculations(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans cal.tsv for funds.calculation_relationships."""
    return (
        df.with_columns(
            pl.col("grp").cast(pl.Int32, strict=False),
            pl.col("arc").cast(pl.Int32, strict=False),
            pl.col("negative").cast(pl.Int16, strict=False).fill_null(0),
        )
        .drop_nulls(subset=["adsh", "grp", "arc", "ptag", "pversion", "ctag", "cversion"])
    )