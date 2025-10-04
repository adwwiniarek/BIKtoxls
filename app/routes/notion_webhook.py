from __future__ import annotations
from fastapi import APIRouter, Header, HTTPException, Request
from ..config import settings

router = APIRouter()

def verify_signature(signature: str | None, raw_body: bytes) -> bool:
    # Placeholder – tu dodaj HMAC wg aktualnej specyfikacji Notion webhooków,
    # jeśli używasz `NOTION_WEBHOOK_SECRET`.
    if settings.webhook_secret:
        # Zaimplementuj w razie potrzeby. Na razie zwracamy True (lub włącz blokadę niżej).
        return True
    return True

@router.post("/webhooks/notion")
async def notion_webhook(request: Request, **headers):
    body = await request.body()
    signature = headers.get("Notion-Signature") or headers.get("X-Notion-Signature")
    if not verify_signature(signature, body):
        raise HTTPException(status_code=401, detail="Invalid signature")
    # Na potrzeby migracji – loguj tylko nagłówki i typy eventów
    try:
        payload = await request.json()
    except Exception:
        payload = {"_raw": body.decode("utf-8", errors="ignore")}
    # Tu mógłbyś obsłużyć np. 'data_source.schema_updated', 'data_source.content_updated'
    return {"ok": True, "received": payload.get("type", "unknown")}
