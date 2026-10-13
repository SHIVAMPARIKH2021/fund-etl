from typing import LiteralString

get_or_create_quarter_config = """
    INSERT INTO sec_financials.quarter_config (year, quarter, is_active, updated_at, updated_by)
    VALUES (%(year)s, %(quarter)s, TRUE, CURRENT_TIMESTAMP, %(updated_by)s)
    ON CONFLICT (year, quarter) DO UPDATE SET
        updated_at = CURRENT_TIMESTAMP
    RETURNING id;
"""

create_temporary_sec_financial_table: LiteralString = """CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP;"""

copy_from_std_in: LiteralString = """COPY {temp_table} ({col_clause}) FROM STDIN {copy_options}"""

sql_insert: LiteralString = """
    INSERT INTO {table_name} ({col_clause})
    SELECT {col_clause} FROM {temp_table}
    ON CONFLICT ({conflict_clause}) DO NOTHING;
"""

copy_quarter_taxonomy_tags = """
    INSERT INTO sec_financials.quarter_taxonomy_tags (quarter_id, tag, version)
    SELECT %(quarter_id)s, tag, version
    FROM tmp_tag_bridge
    ON CONFLICT (quarter_id, tag, version) DO NOTHING;
"""

# Tickers remain unchanged as current market reference data
create_temporary_ticker_table = """
    CREATE TEMP TABLE tmp_tickers (
        LIKE sec_financials.company_tickers_mf INCLUDING DEFAULTS
    ) ON COMMIT DROP;
"""

copy_temporary_ticker_mf = """
    COPY tmp_tickers (cik, series_id, class_id, symbol)
    FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')
"""

insert_tickers = """
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
"""

update_tickers = """
    UPDATE sec_financials.company_tickers_mf
    SET is_active = FALSE
    WHERE is_active = TRUE
      AND last_seen < %(sync_start)s;
"""

insert_pipeline_execution_log = """
    INSERT INTO sec_financials.pipeline_execution_log (
        filing_year,
        filing_quarter,
        ingestion_status,
        ingestion_records,
        ingestion_started_at,
        ingestion_completed_at,
        ingestion_error,
        created_by,
        created_at,
        updated_at
    ) VALUES (
        %(year)s, %(quarter)s, 'IN_PROGRESS', 0, CURRENT_TIMESTAMP, NULL, NULL,
        %(created_by)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (filing_year, filing_quarter) DO UPDATE SET
        ingestion_status = 'IN_PROGRESS',
        ingestion_records = 0,
        ingestion_started_at = CURRENT_TIMESTAMP,
        ingestion_completed_at = NULL,
        ingestion_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    RETURNING log_id;
"""

update_pipeline_execution_log = """
    UPDATE sec_financials.pipeline_execution_log
    SET ingestion_status = %(status)s,
        ingestion_records = %(total_records)s,
        ingestion_error = %(error_message)s,
        ingestion_completed_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE log_id = %(log_id)s;
"""

get_file_ingestion_status = """
    select ingestion_status 
    from sec_financials.pipeline_execution_log
    where filing_quarter =  %(quarter)s
    and filing_year = %(year)s;
"""

create_temporary_tag_bridge_table = """
                CREATE TEMP TABLE tmp_tag_bridge (
                    tag TEXT NOT NULL,
                    version TEXT NOT NULL
                ) ON COMMIT DROP;
"""

copy_temporary_tag_bridge_table = """COPY tmp_tag_bridge (tag, version) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')"""