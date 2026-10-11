# fund_etl/extract/tickers.py
import io
import logging
import httpx
import polars as pl
import psycopg

from datetime import datetime, timezone
from fund_etl.sql.queries import create_temporary_ticker_table, copy_temporary_ticker_mf, insert_tickers, update_tickers
from fund_etl.config import settings

logger = logging.getLogger(__name__)

TICKERS_URL = settings.ticker_url


def sync_mutual_fund_tickers() -> None:
    logger.info("Fetching mutual fund tickers from SEC...")
    headers = {
        "User-Agent": settings.sec_user_agent,
        "Accept-Encoding": "gzip, deflate",
    }

    with httpx.Client(headers=headers, timeout=30.0) as client:
        resp = client.get(TICKERS_URL)
        resp.raise_for_status()
        payload = resp.json()

    data = payload["data"]

    df = pl.DataFrame(
        data,
        schema={
            "cik": pl.Int32,
            "series_id": pl.String,
            "class_id": pl.String,
            "symbol": pl.String,
        },
    )

    total_incoming = len(df)
    logger.info("Ingesting %d mutual fund share classes into database...", total_incoming)

    # Serialize only the 4 incoming columns for COPY
    buffer = io.BytesIO()
    df.write_csv(buffer, separator="\t", include_header=False)
    buffer.seek(0)

    # Capture the exact sync start timestamp for the active watermark
    sync_start_time = datetime.now(timezone.utc)

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            # 1. Create temp table matching structure
            cur.execute(create_temporary_ticker_table)

            # 2. Fast streaming COPY into temp table
            copy_sql = copy_temporary_ticker_mf
            with cur.copy(copy_sql) as copy:
                while chunk := buffer.read(65536):
                    copy.write(chunk)

            # 3. Upsert into target table:
            #    - New rows: insert with is_active = TRUE, first_seen = NOW, last_seen = NOW
            #    - Existing rows: reactivate if was inactive, refresh last_seen to NOW, update symbol/series_id
            cur.execute(insert_tickers)

            # 4. Soft-deactivate entries absent from this SEC payload
            #    Any active record whose last_seen was NOT updated in this run is marked inactive
            cur.execute(update_tickers, {"sync_start": sync_start_time})

            deactivated_count = cur.rowcount
            if deactivated_count > 0:
                logger.info("Marked %d defunct/delisted share classes as inactive.", deactivated_count)

        conn.commit()

    logger.info("Mutual fund tickers synced successfully with audit tracking.")