# app/routes/notion_compat.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
import os
from ..notion_client import NotionClient, NotionError

router = APIRouter()

# GET /notion/poll-one?page_id=...&x_key=...
@router.get("/notion/poll-one")
def notion_poll_one(page_id: str = Query(...), x_key: str | None = Query(None)):
    # opcjonalne prosty „sekret” via env (jeśli ustawisz NOTION_X_KEY w Render)
    required = os.getenv("NOTION_X_KEY")
    if required and x_key != required:
        raise HTTPException(status_code=403, detail="Forbidden")

    client = NotionClient()
    try:
        data = client.get_page(page_id)
    except NotionError as e:
        raise HTTPException(status_code=502, detail=f"Notion error: {e}")

    # zwróć minimalne, stabilne pole + całość dla debug
    return {
        "id": data.get("id"),
        "archived": data.get("archived"),
        "last_edited_time": data.get("last_edited_time"),
        "object": data.get("object"),
        "raw": data,  # na start zostawiamy pełny payload, możesz wyciąć później
    }
