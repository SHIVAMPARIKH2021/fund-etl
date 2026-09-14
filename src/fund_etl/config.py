from pathlib import Path
from pydantic import PostgresDsn, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Base directory pointing to repo root: fund-etl/
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"


class Settings(BaseSettings):
    """
    Centralized pipeline settings.
    Automatically reads from environment variables, falls back to .env,
    and fails loudly at boot if required parameters are missing or invalid.
    """
    # Required parameters (Pipeline crashes immediately on startup if missing)
    database_url: PostgresDsn = Field(
        ..., 
        description="Postgres connection string"
    )
    sec_user_agent: str = Field(
        ..., 
        min_length=10, 
        description="User-Agent required by SEC (e.g., 'AppName admin@domain.com')"
    )

    # Optional defaults
    sec_base_url: str = "https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets"
    ticker_url: str = "https://www.sec.gov/files/company_tickers_mf.json"
    batch_size: int = 250_000
    log_level: str = "INFO"

    # Pydantic v2 Settings configuration
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",  # Allows other system env vars without raising validation errors
    )


# Singleton instance used across modules
settings = Settings()