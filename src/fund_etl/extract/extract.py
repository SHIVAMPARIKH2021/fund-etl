import io
import logging
from collections.abc import Iterator
from pathlib import Path
import zipfile

import httpx
import polars as pl

from fund_etl.config import RAW_DATA_DIR, settings

logger = logging.getLogger(__name__)

SEC_DATASET_FILES = ["sub.tsv", "tag.tsv", "num.tsv", "lab.tsv", "txt.tsv", "cal.tsv"]


def download_and_extract_quarter(
    year: int,
    quarter: int,
    target_base_dir: Path = RAW_DATA_DIR,
    force_download: bool = False,
) -> Path:
    quarter_label = f"{year}q{quarter}"
    file_name = f"{quarter_label}_rr1.zip"
    zip_url = f"{settings.sec_base_url}/{file_name}"
    destination_dir = target_base_dir / quarter_label
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Skip if already unpacked
    all_files_exist = all((destination_dir / f).exists() for f in SEC_DATASET_FILES)
    if all_files_exist and not force_download:
        logger.info("Files for %s already exist. Skipping download.", quarter_label)
        return destination_dir

    headers = {
        "User-Agent": settings.sec_user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }

    zip_file_path = destination_dir / f"{quarter_label}.zip"

    # Set explicit timeouts (connect, read, write, pool)
    timeout_cfg = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=10.0)

    logger.info("Downloading %s to %s...", quarter_label, zip_file_path)
    with httpx.Client(headers=headers, timeout=timeout_cfg, follow_redirects=True) as client:
        with client.stream("GET", zip_url) as response:
            if response.status_code == 404:
                raise FileNotFoundError(f"Dataset not found for {quarter_label} at {zip_url}")
            response.raise_for_status()

            with open(zip_file_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=1048576):  # 1MB chunks
                    f.write(chunk)

    logger.info("Extracting %s...", zip_file_path.name)
    with zipfile.ZipFile(zip_file_path) as zip_ref:
        for member in zip_ref.namelist():
            extracted_path = destination_dir / member
            if not extracted_path.resolve().is_relative_to(destination_dir.resolve()):
                raise RuntimeError(f"Unsafe path: {member}")
            zip_ref.extract(member, destination_dir)

    # Clean up zip file
    zip_file_path.unlink()
    logger.info("Extraction complete for %s.", quarter_label)
    return destination_dir


def scan_tsv_lazy(tsv_path: Path) -> pl.LazyFrame:
    """Builds an optimized LazyFrame scan over an SEC TSV."""
    if not tsv_path.exists():
        raise FileNotFoundError(f"TSV file not found: {tsv_path}")

    return pl.scan_csv(
        tsv_path,
        separator="\t",
        infer_schema_length=20000,
        null_values=["", "none", "null"],
        truncate_ragged_lines=True,
        ignore_errors=True,
    )


def stream_batches(
    tsv_path: Path,
    batch_size: int = 100_000,
) -> Iterator[pl.DataFrame]:
    """Iterates through massive TSVs (e.g., num.tsv, txt.tsv) in bounded memory batches."""
    lazy_plan = scan_tsv_lazy(tsv_path)
    # Uses Polars' streaming execution engine
    return lazy_plan.collect(streaming=True).iter_slices(n_rows=batch_size)