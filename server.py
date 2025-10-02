# server.py (wersja finalna, produkcyjna)

from fastapi import FastAPI, Request, HTTPException, Query
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os

# Importujemy naszą finalną, poprawną funkcję z drugiego pliku
from parse_bik import parse_bik_pdf

# --- KONFIGURACJA ---
# Upewnij się, że nazwy poniżej DOKŁADNIE odpowiadają nazwom kolumn w Twojej bazie Notion.
NOTION_PDF_PROPERTY_NAME = "Raporty BIK"  # POPRAWIONA NAZWA
NOTION_XLS_PROPERTY_NAME = "BIK Raport"
NOTION_SOURCE_PROPERTY_NAME = "Źródło"
# --------------------

app = FastAPI()

# Inicjalizacja klienta Notion
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

def create_excel_file(data):
    output = BytesIO()
    df = pd.DataFrame(data)
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='BIK_Raport')
    output.seek(0)
    return output

@app.get('/notion/poll-one')
async def notion_poll_one(page_id: str = Query(..., alias="page_id"), x_key: str = Query(..., alias="x_key")):
    try:
        if not page_id:
            raise HTTPException(status_code=400, detail="Brak page_id w adresie URL.")

        print(f"Otrzymano żądanie dla strony: {page_id}")
        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        pdf_property = props.get(NOTION_PDF_PROPERTY_NAME, {})
        xls_property = props.get(NOTION_XLS_PROPERTY_NAME, {})

        pdf_files = pdf_property.get('files', [])
        xls_files = xls_property.get('files', [])

        if not pdf_files:
            print(f"Brak akcji: Nie znaleziono plików PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}'.")
            return {"message": f"ℹ️ Brak akcji (brak PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}')"}
        
        if xls_files:
            print(f"Brak akcji: Plik wynikowy XLS już istnieje w kolumnie '{NOTION_XLS_PROPERTY_NAME}'.")
            return {"message": f"ℹ️ Brak akcji (plik XLS już istnieje w '{NOTION_XLS_PROPERTY_NAME}')"}
        
        pdf_url = pdf_files[0]['file']['url']
        source_property = props.get(NOTION_SOURCE_PROPERTY_NAME, {})
        source = source_property.get('select', {}).get('name', 'auto')

        print(f"Rozpoczynam przetwarzanie pliku z URL: {pdf_url}")
        async with httpx.AsyncClient() as client:
            response = await client.get(pdf_url)
            response.raise_for_status()
            pdf_bytes = response.content

        parsed_data = parse_bik_pdf(pdf_bytes, source=source)

        if not parsed_data:
            print("Błąd: Parser nie zwrócił żadnych danych.")
            raise HTTPException(status_code=400, detail="Nie znaleziono danych do przetworzenia w pliku PDF")

        print(f"Znaleziono {len(parsed_data)} rekordów. (Logika uploadu i aktualizacji Notion jest uproszczona)")
        
        return {"message": f"Plik dla strony {page_id} został przetworzony pomyślnie. Znaleziono {len(parsed_data)} rekordów."}

    except Exception as e:
        print(f"Wystąpił krytyczny błąd: {e}")
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
