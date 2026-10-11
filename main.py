# fund_etl/main.py
import argparse
import logging
import os
import shutil
import traceback
from pathlib import Path

import psycopg

from fund_etl.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, settings
from fund_etl.extract.extract import download_and_extract_quarter, scan_tsv_lazy, stream_batches
from fund_etl.extract.extract_ticker import sync_mutual_fund_tickers
from fund_etl.load.load import bulk_copy_dataframe, link_quarter_taxonomy_tags
from fund_etl.load.pipeline_log import (
    mark_pipeline_failed,
    mark_pipeline_success,
    start_pipeline_execution,
)
from fund_etl.sql.queries import get_or_create_quarter_config
from fund_etl.transform.transform import (
    transform_calculations,
    transform_labels,
    transform_numeric_facts,
    transform_submissions,
    transform_tags,
    transform_text_disclosures,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("fund_etl")

# submissions now references quarter_id
SUB_COLS = [
    "adsh", "cik", "name", "countryba", "stprba", "cityba", "zipba",
    "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma",
    "mas1", "mas2", "countryinc", "stprinc", "ein", "former", "changed",
    "fye", "pdate", "effdate", "form", "filed", "accepted", "instance",
    "nciks", "aciks", "quarter_id"
]
TAG_COLS = ["tag", "version", "custom", "abstract", "datatype", "iord", "tlabel", "doc"]
NUM_COLS = [
    "adsh", "tag", "version", "ddate", "uom", "series", "class",
    "measure", "document", "otherdims", "iprx", "value", "footnote",
    "footlen", "dimn", "dcml"
]
TXT_COLS = [
    "adsh", "tag", "version", "ddate", "series", "class", "measure",
    "document", "otherdims", "lang", "iprx", "dcml", "dimn", "escaped",
    "srclen", "txtlen", "footnote", "footlen", "context", "value"
]
LAB_COLS = ["adsh", "tag", "version", "std", "terse", "verbose", "total", "negated", "negatedterse"]
CAL_COLS = ["adsh", "grp", "arc", "negative", "ptag", "pversion", "ctag", "cversion"]


def resolve_quarter_id(year: int, quarter: int) -> int:
    """Fetches or registers the quarter configuration record."""
    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                get_or_create_quarter_config,
                {"year": year, "quarter": quarter, "updated_by": "python-fund-etl"}
            )
            quarter_id = cur.fetchone()[0]
        conn.commit()
    return quarter_id


def run_pipeline(year: int, quarter: int, is_ticker_mf: bool, force: bool = False) -> None:
    logger.info("Executing SEC Fund Prospectus ETL for %dq%d", year, quarter)

    # 1. Resolve quarter_id from quarter_config
    quarter_id = resolve_quarter_id(year, quarter)
    logger.info("Resolved quarter_id: %d for %dq%d", quarter_id, year, quarter)

    # 2. Execution Log
    log_id = start_pipeline_execution(year, quarter)
    total_records = 0

    try:
        q_dir = download_and_extract_quarter(year, quarter, force_download=force)

        # 3. Submissions (Attached to quarter_id)
        logger.info("Ingesting Submissions (sub.tsv)...")
        sub_df = transform_submissions(scan_tsv_lazy(q_dir / "sub.tsv").collect())
        rows = bulk_copy_dataframe(
            sub_df,
            "sec_financials.submissions",
            SUB_COLS,
            conflict_columns=["adsh"],
            extra_columns={"quarter_id": quarter_id},
        )
        total_records += rows

        # 4. Taxonomy Tags (Global catalog + Quarter association bridge)
        logger.info("Ingesting Taxonomy Tags (tag.tsv)...")
        tag_df = transform_tags(scan_tsv_lazy(q_dir / "tag.tsv").collect())
        rows = bulk_copy_dataframe(
            tag_df,
            "sec_financials.taxonomy_tags",
            TAG_COLS,
            conflict_columns=["tag", "version"],
        )
        total_records += rows

        # Register quarter association in quarter_taxonomy_tags bridge
        bridge_rows = link_quarter_taxonomy_tags(tag_df, quarter_id)
        logger.info("Linked %d tags to quarter_id %d", bridge_rows, quarter_id)

        # 5. Numeric Facts
        logger.info("Streaming Numeric Facts (num.tsv)...")
        for idx, raw_chunk in enumerate(stream_batches(q_dir / "num.tsv", batch_size=200_000)):
            clean_chunk = transform_numeric_facts(raw_chunk)
            rows = bulk_copy_dataframe(
                clean_chunk,
                "sec_financials.numeric_facts",
                NUM_COLS,
            )
            total_records += rows
            logger.info("num.tsv chunk %d ingested: %d rows", idx, rows)

        # 6. Text Disclosures
        if (q_dir / "txt.tsv").exists():
            logger.info("Streaming Text Disclosures (txt.tsv)...")
            for idx, raw_chunk in enumerate(stream_batches(q_dir / "txt.tsv", batch_size=100_000)):
                clean_chunk = transform_text_disclosures(raw_chunk)
                rows = bulk_copy_dataframe(
                    clean_chunk,
                    "sec_financials.text_disclosures",
                    TXT_COLS,
                )
                total_records += rows

        # 7. Presentation Labels
        if (q_dir / "lab.tsv").exists():
            logger.info("Ingesting Presentation Labels (lab.tsv)...")
            lab_df = transform_labels(scan_tsv_lazy(q_dir / "lab.tsv").collect())
            rows = bulk_copy_dataframe(
                lab_df,
                "sec_financials.presentation_labels",
                LAB_COLS,
            )
            total_records += rows

        # 8. Calculations
        if (q_dir / "cal.tsv").exists():
            logger.info("Ingesting Calculations (cal.tsv)...")
            cal_df = transform_calculations(scan_tsv_lazy(q_dir / "cal.tsv").collect())
            rows = bulk_copy_dataframe(
                cal_df,
                "sec_financials.calculation_relationships",
                CAL_COLS,
            )
            total_records += rows

        # 9. Company Tickers (Reference SCD, unchanged)
        if is_ticker_mf:
            logger.info("Ingesting Company ticker (company_ticker_mf.json)...")
            sync_mutual_fund_tickers()

        # 10. Clean up raw extract
        logger.info("Copying metadata to processed and cleaning up the raw data...")
        copy_metadata_and_clean_directory(RAW_DATA_DIR, PROCESSED_DATA_DIR)

        mark_pipeline_success(log_id, total_records)
        logger.info("ETL pipeline complete for %dq%d. Ingested %d records.", year, quarter, total_records)

    except Exception as e:
        error_details = traceback.format_exc()
        logger.error("Pipeline failed for %dq%d: %s", year, quarter, e)
        mark_pipeline_failed(log_id, error_details, partial_records=total_records)
        raise


def copy_metadata_and_clean_directory(source: Path, destination: Path):
    if not source.is_dir() or not source.exists():
        raise FileNotFoundError(f"Source directory not found: {source}")
    destination.mkdir(parents=True, exist_ok=True)

    for directory in source.iterdir():
        full_source_path = os.path.join(source, directory)
        full_destination_path = os.path.join(destination, directory.parts[-1])
        if Path(full_source_path).is_dir():
            for file in Path(full_source_path).iterdir():
                if file.is_file() and (file.name.endswith(".json") or file.name.endswith(".htm")):
                    os.makedirs(full_destination_path, exist_ok=True)
                    shutil.copy(file, full_destination_path)
        shutil.rmtree(full_source_path)


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in ("true", "t", "yes", "y", "1"):
        return True
    elif value.lower() in ("false", "f", "no", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected (True/False).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC Fund Prospectus ETL")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2026)")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], required=True, help="Quarter (1-4)")
    parser.add_argument("--ticker", type=str_to_bool, required=True, help="Ticker MF (True/False)")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    args = parser.parse_args()

    run_pipeline(year=args.year, quarter=args.quarter, is_ticker_mf=args.ticker, force=args.force)