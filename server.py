# server.py (Final Version with XLS Download)
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os

from parse_bik import parse_bik_txt

# --- KONFIGURACJA ---
NOTION_TXT_PROPERTY_NAME = "Raporty BIK"
NOTION_XLS_PROPERTY_NAME = "BIK Raport" # Ta kolumna pozostaje nieużywana
NOTION_SOURCE_PROPERTY_NAME = "Źródło"
# --------------------

app = FastAPI()
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

def create_excel_file_stream(data):
    """Tworzy plik Excel w pamięci i zwraca go jako strumień bajtów."""
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

        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        txt_property = props.get(NOTION_TXT_PROPERTY_NAME, {})
        txt_files = txt_property.get('files', [])

        if not txt_files:
            return {"message": f"ℹ️ Brak akcji (brak pliku .TXT w kolumnie '{NOTION_TXT_PROPERTY_NAME}')"}
        
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
        
        # Tworzenie pliku Excel w pamięci
        excel_stream = create_excel_file_stream(parsed_data)
        
        # Nagłówki, które zmuszą przeglądarkę do pobrania pliku
        headers = {
            'Content-Disposition': 'attachment; filename="BIK_Raport.xlsx"'
        }
        
        # Zwrócenie pliku do przeglądarki jako odpowiedź na kliknięcie linku
        return StreamingResponse(excel_stream, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
