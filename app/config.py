import os

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2025-09-03")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Europe/Warsaw")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")

# Opcjonalny klucz do ochrony endpointu (jeśli ustawisz w Render)
X_API_KEY = os.getenv("X_API_KEY", "")
