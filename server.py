# server.py (Wersja dla plików .txt)
from fastapi import FastAPI, Request, HTTPException, Query
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os

# Importujemy nową, uproszczoną funkcję
from parse_bik import parse_bik_txt

# --- KONFIGURACJA ---
NOTION_TXT_PROPERTY_NAME = "Raporty BIK" # Ta kolumna teraz będzie przyjmować pliki .txt
NOTION_XLS_PROPERTY_NAME = "BIK Raport"
NOTION_SOURCE_PROPERTY_NAME = "Źródło"
# --------------------

app = FastAPI()
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

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
            # Odczytujemy plik jako tekst (z kodowaniem utf-8)
            text_content = response.text

        # Wywołujemy nową funkcję, przekazując jej tekst
        parsed_data = parse_bik_txt(text_content, source=source)

        if not parsed_data:
            raise HTTPException(status_code=400, detail="Nie znaleziono danych do przetworzenia w pliku tekstowym.")

        # Logika tworzenia Excela pozostaje bez zmian, ale na razie ją pomijamy
        # ... create_excel_file(parsed_data) ...
        
        return {"message": f"Plik dla strony {page_id} został przetworzony pomyślnie. Znaleziono {len(parsed_data)} rekordów."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
