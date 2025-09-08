import os, io, re, json, time, hmac, base64
from typing import List, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from openpyxl import Workbook
import httpx

from parse_bik import parse_bik_pdf
from notion_client import Client as Notion

# =========================
# KONFIG
# =========================
app = FastAPI(title="BIK PDF -> XLS (Notion bridge + expiring links)")

FILES_DIR = os.getenv("FILES_DIR", "files")
os.makedirs(FILES_DIR, exist_ok=True)
app.mount("/files", StaticFiles(directory=FILES_DIR), name="files")

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
if not PUBLIC_BASE_URL:
    raise RuntimeError("Ustaw PUBLIC_BASE_URL, np. https://twoj-app.onrender.com")

LINK_TTL_DAYS = int(os.getenv("LINK_TTL_DAYS", "7"))
DL_SECRET = os.getenv("DL_SECRET", "")
if not DL_SECRET:
    raise RuntimeError("Ustaw DL_SECRET (losowy sekret do podpisu tokenów)")

NOTION_TOKEN   = os.getenv("NOTION_TOKEN", "")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID", "")
PROP_PDF       = os.getenv("PROP_PDF", "PDF")
PROP_XLS       = os.getenv("PROP_XLS", "XLS")
PROP_STATUS    = os.getenv("PROP_STATUS", "Status")
STATUS_NEW     = os.getenv("STATUS_NEW", "Nowy")
STATUS_DONE    = os.getenv("STATUS_DONE", "Przetworzony")

# =========================
# POMOCNICZE
# =========================
def rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK_Raport"
    headers = [
        "Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
        "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości"
    ]
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "") for h in headers])
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf.read()

def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip()

def save_file(content: bytes, name: str) -> str:
    safe = _safe_name(name)
    path = os.path.join(FILES_DIR, safe)
    with open(path, "wb") as f:
        f.write(content)
    return path

def sign_token(filename: str, exp_ts: int) -> str:
    msg = f"{filename}|{exp_ts}".encode("utf-8")
    sig = hmac.new(DL_SECRET.encode("utf-8"), msg, "sha256").digest()
    payload = filename.encode("utf-8") + b"|" + str(exp_ts).encode("utf-8") + b"|" + base64.urlsafe_b64encode(sig)
    return base64.urlsafe_b64encode(payload).decode("utf-8")

def verify_token(token: str) -> str:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8"))
        parts = raw.split(b"|")
        if len(parts) != 3: raise ValueError("bad parts")
        filename = parts[0].decode("utf-8")
        exp_ts = int(parts[1].decode("utf-8"))
        sig = base64.urlsafe_b64decode(parts[2])
        msg = f"{filename}|{exp_ts}".encode("utf-8")
        good = hmac.new(DL_SECRET.encode("utf-8"), msg, "sha256").digest()
        if not hmac.compare_digest(sig, good):
            raise ValueError("bad signature")
        if time.time() > exp_ts:
            raise ValueError("expired")
        return filename
    except Exception:
        raise HTTPException(status_code=403, detail="Invalid or expired token")

def expiring_download_url(filename: str) -> str:
    exp_ts = int(time.time() + LINK_TTL_DAYS * 24 * 3600)
    token = sign_token(filename, exp_ts)
    return f"{PUBLIC_BASE_URL}/dl?token={token}"

async def http_get_bytes(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=90) as cx:
        r = await cx.get(url)
        r.raise_for_status()
        return r.content

def get_page_title_from_properties(props: Dict[str, Any]) -> str:
    for _, val in props.items():
        if isinstance(val, dict) and val.get("type") == "title":
            t = "".join([x.get("plain_text","") for x in val.get("title", [])]).strip()
            if t:
                return t
    return "Klient"

def extract_file_urls_from_notion_file_prop(file_prop: Dict[str, Any]):
    urls = []
    if not file_prop or file_prop.get("type") != "files":
        return urls
    for f in file_prop.get("files", []):
        if f.get("type") == "file":
            u = f.get("file", {}).get("url")
        else:
            u = f.get("external", {}).get("url")
        if u: urls.append(u)
    return urls

def infer_source_from_name(name_or_url: str) -> str:
    n = (name_or_url or "").lower()
    if re.search(r"(firma|nip)", n): return "firmowy"
    if re.search(r"(prywat|osob)", n): return "prywatny"
    return "auto"

# =========================
# ROUTES
# =========================
@app.get("/")
def root():
    return {"ok": True, "service": "BIK PDF -> XLS", "expiring_days": LINK_TTL_DAYS}

@app.get("/dl")
def download(token: str = Query(...)):
    filename = verify_token(token)
    path = os.path.join(FILES_DIR, _safe_name(filename))
    if not os.path.isfile(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=os.path.basename(path))

@app.post("/parse")
async def parse_endpoint(file: UploadFile = File(...), source_label: str = "auto"):
    if (file.content_type or "").lower() not in {"application/pdf","application/octet-stream"}:
        raise HTTPException(400, "Prześlij plik PDF")
    pdf_bytes = await file.read()
    rows = parse_bik_pdf(pdf_bytes, source_label)
    if not rows:
        return JSONResponse({"ok": False, "rows": 0, "msg": "Brak danych w sekcji 'w trakcie spłaty'."}, status_code=422)
    xlsx = rows_to_xlsx_bytes(rows)
    name = f"bik_{int(time.time())}.xlsx"
    save_file(xlsx, name)
    url = expiring_download_url(name)
    return {"ok": True, "url": url}

# ----- NOTION BRIDGE -----
def get_notion() -> Notion:
    if not NOTION_TOKEN:
        raise RuntimeError("Brak NOTION_TOKEN")
    return Notion(auth=NOTION_TOKEN)

def _num_or_none(v):
    try:
        return float(v) if v not in ("", None) else None
    except:
        return None

def create_creditors_database(notion: Notion, parent_page_id: str, db_name: str) -> str:
    db = notion.databases.create(
        parent={"type": "page_id", "page_id": parent_page_id},
        title=[{"type": "text", "text": {"content": db_name}}],
        properties={
            "Kredytodawca": {"title": {}},
            "Źródło": {"select": {"options": [
                {"name":"prywatny","color":"blue"},
                {"name":"firmowy","color":"green"},
                {"name":"auto","color":"gray"}
            ]}},
            "Rodzaj_produktu": {"rich_text": {}},
            "Zawarcie_umowy": {"rich_text": {}},
            "Pierwotna_kwota": {"number": {"format":"number"}},
            "Pozostało_do_spłaty": {"number": {"format":"number"}},
            "Kwota_raty": {"number": {"format":"number"}},
            "Suma_zaległości": {"number": {"format":"number"}},
        }
    )
    return db["id"]

def insert_creditor_rows(notion: Notion, db_id: str, rows: List[Dict[str, Any]]):
    for r in rows:
        notion.pages.create(
            parent={"database_id": db_id},
            properties={
                "Kredytodawca": {"title":[{"type":"text","text":{"content": str(r.get("Kredytodawca",""))}}]},
                "Źródło": {"select":{"name": str(r.get("Źródło","auto")) or "auto"}},
                "Rodzaj_produktu": {"rich_text":[{"type":"text","text":{"content": str(r.get("Rodzaj_produktu",""))}}]},
                "Zawarcie_umowy": {"rich_text":[{"type":"text","text":{"content": str(r.get("Zawarcie_umowy",""))}}]},
                "Pierwotna_kwota": {"number": _num_or_none(r.get("Pierwotna_kwota"))},
                "Pozostało_do_spłaty": {"number": _num_or_none(r.get("Pozostało_do_spłaty"))},
                "Kwota_raty": {"number": _num_or_none(r.get("Kwota_raty"))},
                "Suma_zaległości": {"number": _num_or_none(r.get("Suma_zaległości"))},
            }
        )

@app.post("/notion/poll")
async def notion_poll():
    if not NOTION_DB_ID:
        raise HTTPException(500, "Brak NOTION_DB_ID")

    notion = get_notion()

    filters = {
        "and": [
            {"property": PROP_PDF, "files": {"is_not_empty": True}},
            {"or": [
                {"property": PROP_STATUS, "select": {"equals": STATUS_NEW}},
                {"property": PROP_XLS, "url": {"is_empty": True}},
            ]}
        ]
    }

    pages = notion.databases.query(
        **{"database_id": NOTION_DB_ID, "filter": filters, "page_size": 20}
    )["results"]

    processed = 0

    for page in pages:
        pid = page["id"]
        props = page.get("properties", {})
        title = get_page_title_from_properties(props)

        pdf_prop = props.get(PROP_PDF, {})
        pdf_urls = extract_file_urls_from_notion_file_prop(pdf_prop)
        if not pdf_urls:
            continue

        all_rows: List[Dict[str, Any]] = []
        for url in pdf_urls:
            try:
                pdf_bytes = await http_get_bytes(url)
                src = infer_source_from_name(url)
                all_rows.extend(parse_bik_pdf(pdf_bytes, source=src))
            except Exception as e:
                print(f"[WARN] Pobieranie/parsowanie nie powiodło się dla {url}: {e}")

        if not all_rows:
            continue

        xlsx_bytes = rows_to_xlsx_bytes(all_rows)
        filename = _safe_name(f"{title}.xlsx")
        save_file(xlsx_bytes, filename)
        public_url = expiring_download_url(filename)

        db_name = f"{title} lista wierzycieli"
        try:
            db_id = create_creditors_database(notion, parent_page_id=pid, db_name=db_name)
            insert_creditor_rows(notion, db_id, all_rows)
        except Exception as e:
            print(f"[WARN] Nie udało się utworzyć bazy wierzycieli dla {title}: {e}")

        update_props: Dict[str, Any] = { PROP_XLS: {"url": public_url} }
        if PROP_STATUS in props:
            update_props[PROP_STATUS] = {"select": {"name": STATUS_DONE}}
        notion.pages.update(page_id=pid, properties=update_props)

        processed += 1

    return {"ok": True, "processed": processed}
