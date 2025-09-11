import os, io, re, time, hmac, base64
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

import httpx
from openpyxl import Workbook
import fitz  # PyMuPDF
from notion_client import Client as Notion
from notion_client.errors import APIResponseError

from parse_bik import parse_bik_pdf

app = FastAPI(title="BIK PDF -> XLS (Notion & Links)")

# =======================
# ENV / KONFIGURACJA
# =======================
FILES_DIR = os.getenv("FILES_DIR", "files")
os.makedirs(FILES_DIR, exist_ok=True)
app.mount("/files", StaticFiles(directory=FILES_DIR), name="files")

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
if not PUBLIC_BASE_URL:
    raise RuntimeError("Ustaw PUBLIC_BASE_URL, np. https://bik-parser.onrender.com")

LINK_TTL_DAYS = int(os.getenv("LINK_TTL_DAYS", "7"))
DL_SECRET     = os.getenv("DL_SECRET", "")
if not DL_SECRET:
    raise RuntimeError("Ustaw DL_SECRET (losowy sekret do podpisu tokenów)")

NOTION_TOKEN  = os.getenv("NOTION_TOKEN", "")
NOTION_DB_ID  = os.getenv("NOTION_DB_ID", "")
if not NOTION_TOKEN or not NOTION_DB_ID:
    print("[WARN] NOTION_TOKEN / NOTION_DB_ID nie ustawione – endpointy Notion zadziałają dopiero po ich ustawieniu.")

# Pola w bazie Klientów
PROP_PDF        = os.getenv("PROP_PDF", "PDF")     # Files & media
PROP_XLS        = os.getenv("PROP_XLS", "XLS")     # URL
PROP_NIP_MAIN   = os.getenv("PROP_NIP_MAIN", "NIP")
PROP_PESEL_MAIN = os.getenv("PROP_PESEL_MAIN", "PESEL")

# (opcjonalne) klucze dostępu dla linków GET
POLL_ONE_KEY = os.getenv("POLL_ONE_KEY", "")  # jeżeli ustawisz, wymagamy ?x_key=...

# =======================
# POMOCNICZE: XLS, linki
# =======================
def rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK_Raport"
    headers = [
        "Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
        "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości","NIP"
    ]
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "") for h in headers])
    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
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
        exp_ts   = int(parts[1].decode("utf-8"))
        sig      = base64.urlsafe_b64decode(parts[2])
        msg      = f"{filename}|{exp_ts}".encode("utf-8")
        good     = hmac.new(DL_SECRET.encode("utf-8"), msg, "sha256").digest()
        if not hmac.compare_digest(sig, good):
            raise ValueError("bad signature")
        if time.time() > exp_ts:
            raise ValueError("expired")
        return filename
    except Exception:
        raise HTTPException(status_code=403, detail="Invalid or expired token")

def expiring_download_url(filename: str) -> str:
    exp_ts = int(time.time() + LINK_TTL_DAYS * 24 * 3600)
    token  = sign_token(filename, exp_ts)
    return f"{PUBLIC_BASE_URL}/dl?token={token}"

async def http_get_bytes(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=90) as cx:
        r = await cx.get(url)
        r.raise_for_status()
        return r.content

# =======================
# POMOCNICZE: Notion
# =======================
def get_notion() -> Notion:
    if not NOTION_TOKEN:
        raise RuntimeError("Brak NOTION_TOKEN")
    return Notion(auth=NOTION_TOKEN)

def get_page_title_from_properties(props: Dict[str, Any]) -> str:
    for _, val in props.items():
        if isinstance(val, dict) and val.get("type") == "title":
            t = "".join([x.get("plain_text","") for x in val.get("title", [])]).strip()
            if t: return t
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

def _num_or_none(v):
    try:
        return float(v) if v not in ("", None) else None
    except:
        return None

def is_empty_prop(prop: Dict[str, Any]) -> bool:
    t = prop.get("type")
    if t == "rich_text":   return len(prop.get("rich_text", [])) == 0
    if t == "title":       return len(prop.get("title", [])) == 0
    if t == "number":      return prop.get("number") is None
    if t == "url":         return not prop.get("url")
    if t == "select":      return prop.get("select") is None
    if t == "multi_select":return len(prop.get("multi_select", [])) == 0
    if t == "date":        return not prop.get("date")
    return False

def set_page_text_prop_if_empty(notion: Notion, page_id: str, props: Dict[str, Any], field_name: str, value: Optional[str]):
    if not value: return
    prop = props.get(field_name)
    if not isinstance(prop, dict): return
    if not is_empty_prop(prop):    return
    t = prop.get("type")
    if t == "rich_text":
        notion.pages.update(page_id=page_id, properties={
            field_name: {"rich_text":[{"type":"text","text":{"content": value}}]}
        })
    elif t == "title":
        notion.pages.update(page_id=page_id, properties={
            field_name: {"title":[{"type":"text","text":{"content": value}}]}
        })
    elif t == "number":
        if value.isdigit():
            notion.pages.update(page_id=page_id, properties={ field_name: {"number": float(value)} })
    else:
        notion.pages.update(page_id=page_id, properties={
            field_name: {"rich_text":[{"type":"text","text":{"content": value}}]}
        })

# =======================
# NIP / PESEL + źródło
# =======================
NIP_CLEAN_RE   = re.compile(r"[^0-9]")
NIP_FIND_RE    = re.compile(r"\bNIP[:\s]*([0-9]{10}|[0-9]{3}[-\s]?[0-9]{3}[-\s]?[0-9]{2}[-\s]?[0-9]{2})\b")
PESEL_FIND_RE1 = re.compile(r"\bPESEL[:\s]*([0-9]{11})\b")
PESEL_FIND_RE2 = re.compile(r"\b([0-9]{11})\b")

def nip_normalize(nip_s: str) -> str:
    return NIP_CLEAN_RE.sub("", nip_s or "")

def nip_valid(nip: str) -> bool:
    nip = nip_normalize(nip)
    if len(nip) != 10 or not nip.isdigit(): return False
    w = [6,5,7,2,3,4,5,6,7]
    s = sum(int(nip[i])*w[i] for i in range(9))
    return (s % 11) == int(nip[9])

def pesel_valid(pesel: str) -> bool:
    if len(pesel) != 11 or not pesel.isdigit(): return False
    w = [1,3,7,9,1,3,7,9,1,3]
    s = sum(int(pesel[i]) * w[i] for i in range(10))
    return (10 - (s % 10)) % 10 == int(pesel[10])

def pdf_first_page_text(pdf_bytes: bytes) -> str:
    text = ""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) > 0:
            text = doc[0].get_text("text") or ""
        doc.close()
    except Exception:
        pass
    return text

def detect_source_and_ids(pdf_bytes: bytes) -> Dict[str, Optional[str]]:
    text = pdf_first_page_text(pdf_bytes)
    src = "firmowy" if "Wskaźnik BIK Moja Firma" in text else "prywatny"

    found_nip = None
    found_pesel = None
    m = NIP_FIND_RE.search(text)
    if m:
        cand = nip_normalize(m.group(1))
        if nip_valid(cand): found_nip = cand

    m = PESEL_FIND_RE1.search(text) or PESEL_FIND_RE2.search(text)
    if m and pesel_valid(m.group(1)):
        found_pesel = m.group(1)

    return {"source": src, "nip": found_nip, "pesel": found_pesel}

# =======================
# Baza „lista wierzycieli”
# =======================
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
            "NIP": {"rich_text": {}},
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
                "NIP": {"rich_text":[{"type":"text","text":{"content": str(r.get("NIP",""))}}]},
            }
        )

def embed_database_inline(notion: Notion, parent_page_id: str, database_id: str, heading: str = "Lista wierzycieli"):
    # Uwaga: jeśli nie chcesz dodatkowego "link_to_page" w treści karty — NIE wywołuj tej funkcji.
    notion.blocks.children.append(
        block_id=parent_page_id,
        children=[
            {"object":"block","type":"heading_2","heading_2":{"rich_text":[{"type":"text","text":{"content":heading}}]}},
            {"object":"block","type":"link_to_page","link_to_page":{"type":"database_id","database_id":database_id}}
        ]
    )

# =======================
# ROUTES
# =======================
@app.get("/")
def root():
    return {"ok": True, "service": "BIK PDF -> XLS", "expiring_days": LINK_TTL_DAYS}

@app.get("/health")
def health():
    return {"ok": True}

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
    # wykryj źródło gdy "auto"
    if source_label == "auto":
        try:
            info = detect_source_and_ids(pdf_bytes)
            source_label = info.get("source") or "auto"
        except Exception:
            pass
    rows = parse_bik_pdf(pdf_bytes, source_label)
    if not rows:
        return JSONResponse({"ok": False, "rows": 0, "msg": "Brak danych w sekcji 'w trakcie spłaty'."}, status_code=422)
    xlsx = rows_to_xlsx_bytes(rows)
    name = f"bik_{int(time.time())}.xlsx"
    save_file(xlsx, name)
    url = expiring_download_url(name)
    return {"ok": True, "url": url}

# ---------- pojedynczy klient: /notion/poll-one ----------
def _assert_key(x_key: Optional[str]):
    if POLL_ONE_KEY and x_key != POLL_ONE_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

async def _process_single_page(notion: Notion, page: dict) -> bool:
    pid   = page["id"]
    props = page.get("properties", {})
    title = get_page_title_from_properties(props)

    pdf_prop = props.get(PROP_PDF, {})
    pdf_urls = extract_file_urls_from_notion_file_prop(pdf_prop)
    if not pdf_urls:
        return False

    all_rows: List[Dict[str, Any]] = []
    page_nip: Optional[str]   = None
    page_pesel: Optional[str] = None

    for url in pdf_urls:
        pdf_bytes = await http_get_bytes(url)
        info = detect_source_and_ids(pdf_bytes)
        src, nip, pesel = info["source"], info["nip"], info["pesel"]

        parsed = parse_bik_pdf(pdf_bytes, source=src)
        if nip and src == "firmowy":
            for r in parsed: r["NIP"] = nip
            page_nip = page_nip or nip
        if pesel and src == "prywatny":
            page_pesel = page_pesel or pesel

        all_rows.extend(parsed)

    if not all_rows:
        return False

    # XLS
    xlsx_bytes = rows_to_xlsx_bytes(all_rows)
    filename   = _safe_name(f"{title}.xlsx")
    save_file(xlsx_bytes, filename)
    public_url = expiring_download_url(filename)

    # Baza „lista wierzycieli” (bez dopinania link_to_page — żeby nie dublować widoku)
    try:
        db_id = create_creditors_database(notion, parent_page_id=pid, db_name=f"{title} lista wierzycieli")
        insert_creditor_rows(notion, db_id, all_rows)
        # embed_database_inline(notion, pid, db_id)  # <- jeśli chcesz dodatkowy link w karcie, odkomentuj
    except Exception as e:
        print(f"[WARN] Nie udało się utworzyć/wypełnić bazy wierzycieli: {e}")

    # Link do XLS
    notion.pages.update(page_id=pid, properties={PROP_XLS: {"url": public_url}})

    # Uzupełnij NIP/PESEL (tylko jeśli puste)
    page_after  = notion.pages.retrieve(page_id=pid)
    props_after = page_after.get("properties", {})
    if PROP_NIP_MAIN in props_after and page_nip:
        set_page_text_prop_if_empty(notion, pid, props_after, PROP_NIP_MAIN, page_nip)
    if PROP_PESEL_MAIN in props_after and page_pesel:
        set_page_text_prop_if_empty(notion, pid, props_after, PROP_PESEL_MAIN, page_pesel)

    return True

@app.get("/notion/poll-one")
async def notion_poll_one(page_id: str, x_key: Optional[str] = Query(default=None)):
    _assert_key(x_key)
    try:
        notion = get_notion()
        page   = notion.pages.retrieve(page_id=page_id)
        did    = await _process_single_page(notion, page)
        body   = f"<p>✅ Gotowe dla <code>{page_id}</code></p>" if did else f"<p>ℹ️ Brak akcji (np. brak PDF, już ma XLS)</p>"
        return HTMLResponse(f"<!doctype html><meta charset='utf-8'><body style='font-family:system-ui'>{body}</body>")
    except Exception as e:
        return HTMLResponse(f"<!doctype html><meta charset='utf-8'><body style='font-family:system-ui'><p>❌ Błąd: {e}</p></body>", status_code=500)

# ---------- batch: /notion/poll ----------
@app.post("/notion/poll")
async def notion_poll():
    try:
        notion = get_notion()
        pages = notion.databases.query(
            database_id=NOTION_DB_ID,
            filter={
                "and": [
                    {"property": PROP_PDF, "files": {"is_not_empty": True}},
                    {"property": PROP_XLS, "url": {"is_empty": True}},
                ]
            },
            page_size=20
        )["results"]

        processed = 0
        for page in pages:
            if await _process_single_page(notion, page):
                processed += 1
        return {"ok": True, "processed": processed}
    except APIResponseError as e:
        return JSONResponse({"ok": False, "where": "notion", "code": getattr(e, "code", None), "message": str(e)}, status_code=500)
    except Exception as e:
        return JSONResponse({"ok": False, "where": "server", "message": str(e)}, status_code=500)

# ---------- proste UI (z animacją) dla batcha ----------
@app.get("/poll-ui")
def poll_ui():
    html = """
<!doctype html>
<html lang="pl">
<head>
  <meta charset="utf-8" />
  <title>Generuj XLS z BIK</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial;
           max-width: 760px; margin: 40px auto; padding: 0 16px; line-height: 1.5; }
    h1 { font-size: 22px; margin: 0 0 14px; }
    p.hint { margin: 0 0 22px; color: #666; }
    button { padding: 10px 16px; font-size: 16px; border-radius: 10px; border: 1px solid #ccc; cursor: pointer; }
    button[disabled] { opacity: .6; cursor: not-allowed; }
    .row { display: flex; align-items: center; gap: 14px; margin: 18px 0; }
    .spinner { width: 40px; height: 40px; border: 4px solid #d0d0d0; border-top-color: #444; border-radius: 50%; animation: spin 1s linear infinite; display: none; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .progress { color: #666; min-height: 1.2em; }
    pre#out { background: #f6f6f6; padding: 12px; white-space: pre-wrap; border-radius: 10px; border: 1px solid #e3e3e3; }
    .ok { color: #0a7a3d; }
    .err { color: #b00020; }
  </style>
</head>
<body>
  <h1>Generuj XLS na podstawie raportów BIK (pdf). Wywołaj POST <code>/notion/poll</code></h1>
  <p class="hint">Przetwórz wszystkich klientów: „PDF” ma plik, „XLS” jest puste.</p>

  <div class="row">
    <button id="btn" aria-label="Uruchom generowanie XLS">Uruchom</button>
    <div id="sp" class="spinner" role="status" aria-live="polite" aria-label="Pracuję"></div>
    <div id="pg" class="progress" aria-live="polite"></div>
  </div>

  <pre id="out" aria-live="polite"></pre>

  <script>
    const btn = document.getElementById('btn');
    const sp  = document.getElementById('sp');
    const pg  = document.getElementById('pg');
    const out = document.getElementById('out');

    let dotsTimer = null;
    function startProgress() {
      sp.style.display = 'inline-block';
      pg.textContent = 'Wysyłam';
      let n = 0;
      dotsTimer = setInterval(() => {
        n = (n + 1) % 4;
        pg.textContent = 'Przetwarzam' + '.'.repeat(n);
      }, 400);
    }
    function stopProgress() {
      sp.style.display = 'none';
      clearInterval(dotsTimer);
      dotsTimer = null;
    }

    btn.onclick = async () => {
      btn.disabled = true;
      out.textContent = '';
      startProgress();
      try {
        const r = await fetch('/notion/poll', { method: 'POST' });
        const txt = await r.text();
        stopProgress();
        if (r.ok) {
          try {
            const j = JSON.parse(txt);
            pg.innerHTML = '<span class="ok">Zakończono</span>';
            out.textContent = JSON.stringify(j, null, 2);
          } catch (e) {
            pg.innerHTML = '<span class="ok">Zakończono</span>';
            out.textContent = txt;
          }
        } else {
          pg.innerHTML = '<span class="err">Błąd HTTP '+r.status+' '+r.statusText+'</span>';
          out.textContent = txt;
        }
      } catch (e) {
        stopProgress();
        pg.innerHTML = '<span class="err">Błąd połączenia</span>';
        out.textContent = 'Błąd fetch: ' + e;
      } finally {
        btn.disabled = false;
      }
    };
  </script>
</body>
</html>
    """
    return HTMLResponse(html)
