# server.py (Final Version for TXT: XLS Download + Notion Table)
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from notion_client import Client
import httpx
import pandas as pd
from io import BytesIO
import os
import re

from parse_bik import parse_bik_txt

NOTION_TXT_PROPERTY_NAME = "Raporty BIK"
NOTION_CLIENT_NAME_PROPERTY = "Name" 

app = FastAPI()
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

def create_excel_file_stream(data):
    output = BytesIO()
    df = pd.DataFrame(data)
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='BIK_Raport')
    output.seek(0)
    return output

def convert_data_to_notion_table(data):
    if not data: return None
    header_keys = list(data[0].keys())
    header_row = {"type": "table_row", "table_row": {"cells": [[{"type": "text", "text": {"content": str(key)}}] for key in header_keys]}}
    data_rows = []
    for item in data:
        row_cells = []
        for key in header_keys:
            value = item.get(key)
            cell_content = [{"type": "text", "text": {"content": str(value) if value is not None else ""}}]
            row_cells.append(cell_content)
        data_rows.append({"type": "table_row", "table_row": {"cells": row_cells}})
    return {"type": "table", "table": {"table_width": len(header_keys), "has_column_header": True, "has_row_header": False, "children": [header_row, *data_rows]}}

def update_notion_page_with_table(page_id: str, data: list):
    try:
        notion_table_block = convert_data_to_notion_table(data)
        if notion_table_block:
            notion.blocks.children.append(block_id=page_id, children=[notion_table_block])
            print(f"Pomyślnie dodano tabelę do strony Notion: {page_id}")
    except Exception as e:
        print(f"Błąd podczas aktualizacji strony Notion {page_id} w tle: {e}")

@app.get('/notion/poll-one')
async def notion_poll_one(page_id: str = Query(..., alias="page_id"), x_key: str = Query(..., alias="x_key"), background_tasks: BackgroundTasks = BackgroundTasks()):
    try:
        if not page_id:
            raise HTTPException(status_code=400, detail="Brak page_id w adresie URL.")

        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        txt_property = props.get(NOTION_TXT_PROPERTY_NAME, {})
        txt_files = txt_property.get('files', [])

        if not txt_files:
            return {"message": f"ℹ️ Brak akcji (brak plików .TXT w kolumnie '{NOTION_TXT_PROPERTY_NAME}')"}
        
        all_parsed_data = []
        async with httpx.AsyncClient() as client:
            for file_obj in txt_files:
                txt_url = file_obj['file']['url']
                file_name = file_obj.get('name', '').lower()
                
                source = "prywatny"
                if "firmowy" in file_name:
                    source = "firmowy"

                response = await client.get(txt_url)
                response.raise_for_status()
                text_content = response.text

                parsed_data = parse_bik_txt(text_content, source=source)
                if parsed_data:
                    all_parsed_data.extend(parsed_data)

        if not all_parsed_data:
            raise HTTPException(status_code=400, detail="Nie znaleziono danych do przetworzenia w podanych plikach tekstowych.")
        
        background_tasks.add_task(update_notion_page_with_table, page_id, all_parsed_data)

        client_name = "Raport"
        name_property = props.get(NOTION_CLIENT_NAME_PROPERTY, {}).get('title', [])
        if name_property:
            client_name = name_property[0].get('plain_text', 'Raport')
        
        safe_client_name = re.sub(r'[\\/*?:"<>|]', "", client_name)
        filename = f"{safe_client_name} wierzytelnosci.xlsx"
        
        excel_stream = create_excel_file_stream(all_parsed_data)
        
        headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
        
        return StreamingResponse(excel_stream, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")
