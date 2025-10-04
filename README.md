# Restrukturyzacja – Notion (data_sources) – Render deploy

Minimalny, produkcyjny serwis **FastAPI + Uvicorn** pod Render.com,
zaktualizowany pod zmianę Notion API z **2025‑09‑03** (przejście z `database_id` na `data_source_id`).

## Co tu jest
- `app/main.py` – FastAPI app, rejestr tras.
- `app/config.py` – konfiguracja (env), nagłówki Notion (`Notion-Version: 2025-09-03`).
- `app/notion_client.py` – cienki klient Notion z obsługą **/v1/data_sources/**, `pages.create` z `parent.data_source_id`, `search` filtrowane na `data_source`.
- `app/routes/*` – endpointy:
  - `GET /healthz` – zdrowie,
  - `POST /ingest/bik` – przyjmuje JSON (lista zobowiązań) i zapisuje do Notion,
  - `POST /webhooks/notion` – webhook Notion (opcjonalna walidacja podpisu).
- `services/bik_parser.py` – stub parsera; możesz go podmienić na właściwą logikę BIK pdt→xls.

## Uruchomienie lokalne
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Render deploy
- **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- **Health Check Path**: `/healthz`
- **Env** – patrz sekcja poniżej lub `render.yaml`.

## Wymagane zmienne środowiskowe
| Nazwa | Opis |
|---|---|
| `NOTION_TOKEN` | Secret integracji Notion (Internal Integration Token) |
| `NOTION_VERSION` | Wymuś `2025-09-03` (domyślnie tak jest) |
| `DEFAULT_TIMEZONE` | Domyślnie `Europe/Warsaw` |
| `ALLOW_ORIGINS` | Lista CORS (np. `*` albo `https://twojadomena`) |
| `LOG_LEVEL` | `INFO`/`DEBUG` |
| (opcjonalnie) `NOTION_DATA_SOURCE_ID_DEBTS` | `data_source_id` bazy „Zobowiązania” |
| (opcjonalnie) `NOTION_DATA_SOURCE_ID_BANKS` | `data_source_id` bazy „Banki/BIK” |
| (opcjonalnie) `NOTION_WEBHOOK_SECRET` | podpis webhooków Notion (jeśli włączysz) |

> Jeśli znasz tylko `database_id`, endpoint `GET /v1/databases/:id` w wersji `2025-09-03` zwraca listę `data_sources`; klient zrobi „discovery” i wybierze właściwe źródło, gdy nie podasz `*_DATA_SOURCE_ID_*`.

## Format payloadu `/ingest/bik`
```json
{
  "debts": [
    {
      "creditor": "Bank X",
      "amount_pln": 12345.67,
      "status": "aktywne",
      "account_no": "12 3456 ...",
      "due_date": "2025-12-31",
      "type": "kredyt ratalny",
      "notes": "z BIK"
    }
  ]
}
```

Serwis spróbuje utworzyć Strony w bazie powiązanej z `NOTION_DATA_SOURCE_ID_DEBTS` (jeśli nie ustawiono – spróbuje autodetekcji po `NOTION_DATABASE_ID_DEBTS`, a jeśli i tego nie ma – zwróci błąd 422 z podpowiedzią).

## Plik `render.yaml`
Do szybkiego wdrożenia jako **Blueprint**.
