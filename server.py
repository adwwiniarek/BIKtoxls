# server.py (Final Version with Correct Notion Table Generation)
from fastapi import FastAPI, Request, HTTPException, Query
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os

from parse_bik import parse_bik_txt

# --- KONFIGURACJA ---
NOTION_TXT_PROPERTY_NAME = "Raporty BIK"
NOTION_XLS_PROPERTY_NAME = "BIK Raport" # Ta kolumna pozostaje na razie nieużywana
NOTION_SOURCE_PROPERTY_NAME = "Źródło"
# --------------------

app = FastAPI()
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

# === POPRAWIONA FUNKCJA ===
def convert_data_to_notion_table(data):
    """Konwertuje listę słowników na blok tabeli zgodny z API Notion."""
    header_keys = list(data[0].keys())
    
    # Pierwszy wiersz to nagłówek
    header_row = {
        "type": "table_row",
        "table_row": {
            "cells": [[{"type": "text", "text": {"content": str(key)}}] for key in header_keys]
        }
    }
    
    # Reszta wierszy to dane
    data_rows = []
    for item in data:
        row_cells = []
        for key in header_keys:
            value = item.get(key)
            # Każda komórka musi być listą obiektów rich_text
            cell_content = [{"type": "text", "text": {"content": str(value) if value is not None else ""}}]
            row_cells.append(cell_content)
        data_rows.append({
            "type": "table_row",
            "table_row": {
                "cells": row_cells
            }
        })

    # Złożenie finalnego bloku tabeli
    return {
        "type": "table",
        "table": {
            "table_width": len(header_keys),
            "has_column_header": True, # Ta opcja pogrubi nagłówek w Notion
            "has_row_header": False,
            "children": [
                header_row,
                *data_rows
            ]
        }
    }

@app.get('/notion/poll-one')
async def notion_poll_one(page_id: str = Query(..., alias="page_id"), x_key: str = Query(..., alias="x_key")):
    try:
        if not page_id:
            raise HTTPException(status_code=400, detail="Brak page_id w adresie URL.")

        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        txt_property = props.get(NOTION_TXT_PROPERTY_NAME, {})
        xls_property = props.get(NOTION_XLS_PROPERTY_NAME, {})
        txt_files = txt_property.get('files', [])
        xls_files = xls_property.get('files', [])

        if not txt_files:
            return {"message": f"ℹ️ Brak akcji (brak pliku .TXT w kolumnie '{NOTION_TXT_PROPERTY_NAME}')"}
        if xls_files:
            return {"message": f"ℹ️ Brak akcji (plik XLS już istnieje w '{NOTION_XLS_PROPERTY_NAME}')"}
        
        txt_url = txt_files[0]['file']['url']
        source_property = props.get(NOTION_SOURCE_PROPERTY_NAME, {})
        source = source_property.get('select', {}).get('name', 'auto')

        async with httpx.AsyncClient() as client:
            response = await client.get(txt_url)
            response.raise_for_status()
            text_content = response.text

        parsed_data = parse_bik_txt(text_content, source=source)

        if not parsed_data:
            raise HTTPException(status_code=400, detail="Nie znaleziono danych do przetworzenia w pliku tekstowym.")

        # KROK FINALNY: Dodaj przetworzone dane jako blok tabeli na stronie Notion
        notion_table_block = convert_data_to_notion_table(parsed_data)
        notion.blocks.children.append(
            block_id=page_id,
            children=[notion_table_block]
        )
        
        return {"message": f"Gotowe! Dane zostały dodane jako tabela na stronie Notion. Znaleziono {len(parsed_data)} rekordów."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
