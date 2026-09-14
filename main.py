import argparse
import logging
import os
import shutil
from pathlib import Path as Path
from fund_etl.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from fund_etl.extract.extract import download_and_extract_quarter, scan_tsv_lazy, stream_batches
from fund_etl.extract.extract_ticker import sync_mutual_fund_tickers
from fund_etl.load.load import bulk_copy_dataframe
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

SUB_COLS = [
    "adsh", "cik", "name", "countryba", "stprba", "cityba", "zipba",
    "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma",
    "mas1", "mas2", "countryinc", "stprinc", "ein", "former", "changed",
    "fye", "pdate", "effdate", "form", "filed", "accepted", "instance",
    "nciks", "aciks"
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


def run_pipeline(year: int, quarter: int, is_ticker_mf:bool, force: bool = False) -> None:
    logger.info("Executing SEC Fund Prospectus ETL for %dq%d", year, quarter)
    q_dir = download_and_extract_quarter(year, quarter, force_download=force)

    # 1. Submissions (PK: adsh)
    logger.info("Ingesting Submissions (sub.tsv)...")
    sub_df = transform_submissions(scan_tsv_lazy(q_dir / "sub.tsv").collect())
    bulk_copy_dataframe(
        sub_df, 
        "sec_financials.submissions", 
        SUB_COLS, 
        conflict_columns=["adsh"]
    )

    # 2. Taxonomy Tags (PK: tag, version)
    logger.info("Ingesting Taxonomy Tags (tag.tsv)...")
    tag_df = transform_tags(scan_tsv_lazy(q_dir / "tag.tsv").collect())
    bulk_copy_dataframe(
        tag_df, 
        "sec_financials.taxonomy_tags", 
        TAG_COLS, 
        conflict_columns=["tag", "version"]
    )

    # 3. Numeric Facts (No unique constraint in ELT staging, keep direct COPY)
    logger.info("Streaming Numeric Facts (num.tsv)...")
    for idx, raw_chunk in enumerate(stream_batches(q_dir / "num.tsv", batch_size=200_000)):
        clean_chunk = transform_numeric_facts(raw_chunk)
        rows = bulk_copy_dataframe(clean_chunk, "sec_financials.numeric_facts", NUM_COLS)
        logger.info("num.tsv chunk %d ingested: %d rows", idx, rows)

    # 4. Text Disclosures
    if (q_dir / "txt.tsv").exists():
        logger.info("Streaming Text Disclosures (txt.tsv)...")
        for idx, raw_chunk in enumerate(stream_batches(q_dir / "txt.tsv", batch_size=100_000)):
            clean_chunk = transform_text_disclosures(raw_chunk)
            bulk_copy_dataframe(clean_chunk, "sec_financials.text_disclosures", TXT_COLS)

    # 5. Labels (includes "verbose")
    if (q_dir / "lab.tsv").exists():
        logger.info("Ingesting Presentation Labels (lab.tsv)...")
        lab_df = transform_labels(scan_tsv_lazy(q_dir / "lab.tsv").collect())
        bulk_copy_dataframe(lab_df, "sec_financials.presentation_labels", LAB_COLS)

    # 6. Calculations
    if (q_dir / "cal.tsv").exists():
        logger.info("Ingesting Calculations (cal.tsv)...")
        cal_df = transform_calculations(scan_tsv_lazy(q_dir / "cal.tsv").collect())
        bulk_copy_dataframe(cal_df, "sec_financials.calculation_relationships", CAL_COLS)

    # 7. Company tickers
    if is_ticker_mf:
        print(f"Value of is_ticker_mf:{is_ticker_mf}")
        logger.info("Ingesting Company ticker (company_ticker_mf.json)...")
        sync_mutual_fund_tickers()

    #8 Directory cleanup
    logger.info("Copying metadata to processed and cleaning up the raw data...")
    copy_metadata_and_clean_directory(RAW_DATA_DIR, PROCESSED_DATA_DIR)

    logger.info("ETL pipeline complete for %dq%d.", year, quarter)

def copy_metadata_and_clean_directory(source: Path, destination: Path):
    """Function to move a directory to another directory"""
    if not source.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source}")
    if not source.exists():
        raise FileNotFoundError(f"Source directory does not exists: {source}")
    if not destination.is_dir():
        raise FileNotFoundError(f"Destination directory already exists: {destination}")
    if not destination.exists():
        raise FileNotFoundError(f"Destination directory does not exists: {destination}")

    for directory in source.iterdir():
        full_source_path = os.path.join(source, directory)
        full_destination_path = os.path.join(destination, directory.parts[-1])
        if Path(full_source_path).is_dir():
            for file in Path(full_source_path).iterdir():
                if file.is_file() and (file.name.endswith("json")) or (file.name.endswith("htm")):
                    if not Path(full_destination_path).is_dir() and not Path(full_destination_path).exists():
                        os.mkdir(full_destination_path)
                    shutil.copy(file, full_destination_path)
        shutil.rmtree(full_source_path)

def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in ('true', 't', 'yes', 'y', '1'):
        return True
    elif value.lower() in ('false', 'f', 'no', 'n', '0'):
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

    run_pipeline(year=args.year, quarter=args.quarter,  is_ticker_mf=args.ticker, force=args.force)