# server.py (wersja diagnostyczna)
from fastapi import FastAPI, Request, HTTPException, Query
from notion_client import Client
import httpx
import os

# Importujemy nową funkcję diagnostyczną
from parse_bik import diagnose_pdf_text

# --- KONFIGURACJA ---
NOTION_PDF_PROPERTY_NAME = "Raporty BIK"
# --------------------

app = FastAPI()
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

@app.get('/notion/poll-one')
async def notion_poll_one_diagnostic(page_id: str = Query(..., alias="page_id"), x_key: str = Query(..., alias="x_key")):
    try:
        if not page_id:
            raise HTTPException(status_code=400, detail="Brak page_id w adresie URL.")

        print(f"DIAGNOSTYKA: Otrzymano żądanie dla strony: {page_id}")
        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        pdf_property = props.get(NOTION_PDF_PROPERTY_NAME, {})
        pdf_files = pdf_property.get('files', [])

        if not pdf_files:
            raise HTTPException(status_code=400, detail=f"DIAGNOZA: Nie znaleziono plików PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}'.")
        
        pdf_url = pdf_files[0]['file']['url']

        print(f"DIAGNOSTYKA: Pobieram plik z URL: {pdf_url}")
        async with httpx.AsyncClient() as client:
            response = await client.get(pdf_url)
            response.raise_for_status()
            pdf_bytes = response.content

        print("DIAGNOSTYKA: Uruchamiam funkcję diagnostyczną na pliku PDF.")
        # Wywołanie nowej funkcji diagnostycznej
        diagnostic_text = diagnose_pdf_text(pdf_bytes)
        
        # Zwróć wynik diagnozy bezpośrednio jako treść błędu.
        # To pozwoli nam zobaczyć, co jest w środku PDF.
        raise HTTPException(status_code=400, detail=diagnostic_text)

    except Exception as e:
        print(f"Wystąpił krytyczny błąd: {e}")
        # Przekaż dalej oryginalny błąd, jeśli wystąpił przed diagnozą
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
