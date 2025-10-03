# server.py (Final Intelligent Error Version)
from fastapi import FastAPI, Request, HTTPException, Query
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os

from parse_bik import parse_bik_pdf

NOTION_PDF_PROPERTY_NAME = "Raporty BIK"
NOTION_XLS_PROPERTY_NAME = "BIK Raport"
NOTION_SOURCE_PROPERTY_NAME = "Źródło"

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
        
        pdf_property = props.get(NOTION_PDF_PROPERTY_NAME, {})
        xls_property = props.get(NOTION_XLS_PROPERTY_NAME, {})
        pdf_files = pdf_property.get('files', [])
        xls_files = xls_property.get('files', [])

        if not pdf_files:
            return {"message": f"ℹ️ Brak akcji (brak PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}')"}
        if xls_files:
            return {"message": f"ℹ️ Brak akcji (plik XLS już istnieje w '{NOTION_XLS_PROPERTY_NAME}')"}
        
        pdf_url = pdf_files[0]['file']['url']
        source_property = props.get(NOTION_SOURCE_PROPERTY_NAME, {})
        source = source_property.get('select', {}).get('name', 'auto')

        async with httpx.AsyncClient() as client:
            response = await client.get(pdf_url)
            response.raise_for_status()
            pdf_bytes = response.content

        result_dict = parse_bik_pdf(pdf_bytes, source=source)
        parsed_data = result_dict.get("rows")
        lines_read = result_dict.get("lines_read", 0)
        active_lines_found = result_dict.get("active_lines_found", False)

        if not parsed_data:
            if lines_read < 10:
                error_detail = "DIAGNOZA: Parser nie był w stanie odczytać prawie żadnego tekstu z pliku PDF. Plik jest najprawdopodobniej skanem (obrazem)."
            elif not active_lines_found:
                error_detail = f"DIAGNOZA: Odczytano {lines_read} linii tekstu z PDF, ale NIE ZNALEZIONO w nich sekcji 'Zobowiązania finansowe - w trakcie spłaty'. Sprawdź, czy na pewno ten raport zawiera tę sekcję lub czy nie ma ona innej nazwy."
            else:
                error_detail = f"DIAGNOZA: Odczytano {lines_read} linii i ZNALEZIONO sekcję 'Zobowiązania...', ale wewnątrz tej sekcji nie znaleziono żadnych linii z datami w formacie DD.MM.RRRR. Sprawdź format dat w pliku."
            raise HTTPException(status_code=400, detail=error_detail)

        return {"message": f"Plik dla strony {page_id} został przetworzony pomyślnie. Znaleziono {len(parsed_data)} rekordów."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
