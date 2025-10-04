from pydantic import BaseModel
import os

class Settings(BaseModel):
    notion_token: str = os.getenv("NOTION_TOKEN", "")
    notion_version: str = os.getenv("NOTION_VERSION", "2025-09-03")
    default_timezone: str = os.getenv("DEFAULT_TIMEZONE", "Europe/Warsaw")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    allow_origins: str = os.getenv("ALLOW_ORIGINS", "*")

    # Opcjonalnie – znane data_source_id dla wygody
    ds_debts: str | None = os.getenv("NOTION_DATA_SOURCE_ID_DEBTS")
    ds_banks: str | None = os.getenv("NOTION_DATA_SOURCE_ID_BANKS")

    webhook_secret: str | None = os.getenv("NOTION_WEBHOOK_SECRET")

settings = Settings()
