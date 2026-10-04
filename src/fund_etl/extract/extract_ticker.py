# fund_etl/extract/tickers.py
import io
import logging
from datetime import datetime, timezone

import httpx
import polars as pl
import psycopg

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
            cur.execute("""
                CREATE TEMP TABLE tmp_tickers (
                    LIKE sec_financials.company_tickers_mf INCLUDING DEFAULTS
                ) ON COMMIT DROP;
            """)

            # 2. Fast streaming COPY into temp table
            copy_sql = """
                COPY tmp_tickers (cik, series_id, class_id, symbol)
                FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')
            """
            with cur.copy(copy_sql) as copy:
                while chunk := buffer.read(65536):
                    copy.write(chunk)

            # 3. Upsert into target table:
            #    - New rows: insert with is_active = TRUE, first_seen = NOW, last_seen = NOW
            #    - Existing rows: reactivate if was inactive, refresh last_seen to NOW, update symbol/series_id
            cur.execute("""
                INSERT INTO sec_financials.company_tickers_mf (
                    cik,
                    series_id,
                    class_id,
                    symbol,
                    is_active,
                    first_seen,
                    last_seen
                )
                SELECT 
                    cik,
                    series_id,
                    class_id,
                    symbol,
                    TRUE,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                FROM tmp_tickers
                ON CONFLICT (class_id) DO UPDATE SET
                    symbol     = EXCLUDED.symbol,
                    series_id  = EXCLUDED.series_id,
                    cik        = EXCLUDED.cik,
                    is_active  = TRUE,
                    last_seen  = CURRENT_TIMESTAMP;
            """)

            # 4. Soft-deactivate entries absent from this SEC payload
            #    Any active record whose last_seen was NOT updated in this run is marked inactive
            cur.execute("""
                UPDATE sec_financials.company_tickers_mf
                SET is_active = FALSE
                WHERE is_active = TRUE
                  AND last_seen < %(sync_start)s;
            """, {"sync_start": sync_start_time})

            deactivated_count = cur.rowcount
            if deactivated_count > 0:
                logger.info("Marked %d defunct/delisted share classes as inactive.", deactivated_count)

        conn.commit()

    logger.info("Mutual fund tickers synced successfully with audit tracking.")