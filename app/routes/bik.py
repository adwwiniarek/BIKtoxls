from __future__ import annotations
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from ..config import settings
from ..notion_client import NotionClient, NotionError
from services.bik_parser import parse_payload_to_debts

router = APIRouter()

class DebtIn(BaseModel):
    creditor: str = Field(..., description="Nazwa wierzyciela")
    amount_pln: float = Field(..., description="Kwota w PLN")
    status: str | None = Field(None, description="np. aktywne, wypowiedziane")
    account_no: str | None = None
    due_date: str | None = None  # YYYY-MM-DD
    type: str | None = None
    notes: str | None = None

class IngestPayload(BaseModel):
    debts: List[DebtIn]
    database_id_debts: Optional[str] = Field(None, description="opcjonalnie – jeśli chcesz, by serwis sam wyszukał data_source")
    data_source_id_debts: Optional[str] = Field(None, description="jeśli znasz – podaj bezpośrednio")

@router.post("/ingest/bik")
def ingest_bik(payload: IngestPayload = Body(...)):
    client = NotionClient()
    # wybór data source – preferuj env, potem payload.ds_id, potem discovery po database_id
    ds_id = settings.ds_debts or payload.data_source_id_debts
    if not ds_id:
        dbid = payload.database_id_debts or None
        if not dbid:
            raise HTTPException(status_code=422, detail="Podaj data_source_id_debts lub database_id_debts (albo skonfiguruj NOTION_DATA_SOURCE_ID_DEBTS w env).")
        try:
            ds_id = client.choose_data_source(dbid, None)
        except NotionError as e:
            raise HTTPException(status_code=500, detail=str(e))

    debts = parse_payload_to_debts(payload.model_dump())
    created = []
    for d in debts:
        # mapowanie pól do właściwości Notion – dostosuj do schematu bazy „Zobowiązania”
        props: Dict[str, Any] = {
            "Nazwa wierzyciela": {"title": [{"text": {"content": d["creditor"]}}]},
            "Kwota (PLN)": {"number": float(d["amount_pln"])},
        }
        if d.get("status"):
            props["Status"] = {"select": {"name": d["status"]}}
        if d.get("type"):
            props["Typ"] = {"select": {"name": d["type"]}}
        if d.get("account_no"):
            props["Rachunek"] = {"rich_text": [{"text": {"content": d["account_no"]}}]}
        if d.get("due_date"):
            props["Termin"] = {"date": {"start": d["due_date"]}}
        if d.get("notes"):
            props["Uwagi"] = {"rich_text": [{"text": {"content": d["notes"]}}]}

        try:
            page = client.create_page(data_source_id=ds_id, properties=props)
            created.append({"page_id": page.get("id")})
        except NotionError as e:
            raise HTTPException(status_code=502, detail=f"Błąd Notion: {e}")

    return {"created": created, "count": len(created)}
