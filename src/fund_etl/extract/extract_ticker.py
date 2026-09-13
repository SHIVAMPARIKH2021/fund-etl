# fund_etl/extract/tickers.py
import io
import logging
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

    fields = [col.lower() for col in payload["fields"]]  # ["cik", "seriesid", "classid", "symbol"]
    data = payload["data"]

    df = pl.DataFrame(data, schema={
        "cik": pl.Int32,
        "series_id": pl.String,
        "class_id": pl.String,
        "symbol": pl.String
    })

    logger.info("Ingesting %d mutual fund share classes into database...", len(df))

    buffer = io.BytesIO()
    df.write_csv(buffer, separator="\t", include_header=False)
    buffer.seek(0)

    with psycopg.connect(str(settings.database_url)) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_tickers (LIKE sec_financials.company_tickers_mf INCLUDING DEFAULTS) 
                ON COMMIT DROP;
            """)
            with cur.copy(
                    "COPY tmp_tickers (cik, series_id, class_id, symbol) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')") as copy:
                while chunk := buffer.read(65536):
                    copy.write(chunk)

            cur.execute("""
                INSERT INTO sec_financials.company_tickers_mf (cik, series_id, class_id, symbol)
                SELECT cik, series_id, class_id, symbol FROM tmp_tickers
                ON CONFLICT (class_id) DO UPDATE 
                SET symbol = EXCLUDED.symbol, series_id = EXCLUDED.series_id;
            """)
        conn.commit()
    logger.info("Mutual fund tickers synced successfully.")