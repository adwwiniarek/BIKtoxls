import os, io, re, time, hmac, base64
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from openpyxl import Workbook
import httpx
import fitz  # PyMuPDF

from parse_bik import parse_bik_pdf
from notion_client import Client as Notion
from notion_client.errors import APIResponseError

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

# Nazwy kolumn w głównej bazie Klientów (Notion)
PROP_PDF         = os.getenv("PROP_PDF", "PDF")    # Files & media
PROP_XLS         = os.getenv("PROP_XLS", "XLS")    # URL
PROP_NIP_MAIN    = os.getenv("PROP_NIP_MAIN", "NIP")
PROP_PESEL_MAIN  = os.getenv("PROP_PESEL_MAIN", "PESEL")
PROP_FIRMA_NAME  = os.getenv("PROP_FIRMA_NAME", "Nazwa_firmy")
PROP_REGON       = os.getenv("PROP_REGON", "REGON")
PROP_CITY        = os.getenv("PROP_CITY", "Miejscowość")
PROP_PKD         = os.getenv("PROP_PKD", "PKD")
PROP_ADDRESS     = os.getenv("PROP_ADDRESS", "Adres")

# Kolumny w tabeli „lista wierzycieli” i w XLS (tylko NIP + Adres z dodatkowych)
CRED_XLS_HEADERS = [
    "Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
    "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości",
    "NIP","Adres"
]

# =========================
# XLS helpers
# =========================
def rows_to_xlsx_bytes(rows: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK_Raport"
    headers = CRED_XLS_HEADERS
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

# =========================
# Notion helpers
# =========================
def get_notion() -> Notion:
    if not NOTION_TOKEN:
        raise RuntimeError("Brak NOTION_TOKEN")
    return Notion(auth=NOTION_TOKEN)

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

def _num_or_none(v):
    try:
        return float(v) if v not in ("", None) else None
    except:
        return None

def is_empty_prop(prop: Dict[str, Any]) -> bool:
    t = prop.get("type")
    if t == "rich_text":
        return len(prop.get("rich_text", [])) == 0
    if t == "title":
        return len(prop.get("title", [])) == 0
    if t == "number":
        return prop.get("number") is None
    if t == "url":
        return not prop.get("url")
    if t == "select":
        return prop.get("select") is None
    if t == "multi_select":
        return len(prop.get("multi_select", [])) == 0
    if t == "date":
        return not prop.get("date")
    return False

def set_page_text_prop_if_empty(notion: Notion, page_id: str, props: Dict[str, Any], field_name: str, value: Optional[str]):
    if not value:
        return
    prop = props.get(field_name)
    if not isinstance(prop, dict):
        return
    if not is_empty_prop(prop):
        return
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

# =========================
# PDF → źródło + NIP/PESEL
# =========================
NIP_CLEAN_RE   = re.compile(r"[^0-9]")
NIP_FIND_RE    = re.compile(r"\bNIP[:\s]*([0-9]{10}|[0-9]{3}[-\s]?[0-9]{3}[-\s]?[0-9]{2}[-\s]?[0-9]{2})\b")
PESEL_FIND_RE1 = re.compile(r"\bPESEL[:\s]*([0-9]{11})\b")
PESEL_FIND_RE2 = re.compile(r"\b([0-9]{11})\b")  # fallback

def nip_normalize(nip_s: str) -> str:
    return NIP_CLEAN_RE.sub("", nip_s or "")

def nip_valid(nip: str) -> bool:
    nip = nip_normalize(nip)
    if len(nip) != 10 or not nip.isdigit():
        return False
    w = [6,5,7,2,3,4,5,6,7]
    s = sum(int(nip[i])*w[i] for i in range(9))
    return (s % 11) == int(nip[9])

def pesel_valid(pesel: str) -> bool:
    if len(pesel) != 11 or not pesel.isdigit():
        return False
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
        if nip_valid(cand):
            found_nip = cand

    m = PESEL_FIND_RE1.search(text)
    if not m:
        m = PESEL_FIND_RE2.search(text)
    if m and pesel_valid(m.group(1)):
        found_pesel = m.group(1)

    return {"source": src, "nip": found_nip, "pesel": found_pesel}

# =========================
# Firma po NIP (API officeblog.pl) – Nazwa, REGON, Miejscowość, PKD, Adres
# =========================
async def fetch_company_data_by_nip(nip: str) -> Dict[str, Any]:
    import xml.etree.ElementTree as ET
    url = "https://api.officeblog.pl/gus.php"
    params = {"NIP": nip, "format": "2"}
    headers = {"Accept": "application/xml"}
    async with httpx.AsyncClient(timeout=30) as cx:
        r = await cx.get(url, params=params, headers=headers)
        r.raise_for_status()
        xml_text = r.text

    def get_first(el: ET.Element, names: list[str]) -> Optional[str]:
        for n in names:
            x = el.find(".//" + n)
            if x is not None and (x.text or "").strip():
                return x.text.strip()
        return None

    root = ET.fromstring(xml_text)
    nazwa = get_first(root, ["Nazwa", "NazwaPodmiotu", "nazwa", "NazwaPelna", "NazwaJednostki"])
    regon = get_first(root, ["REGON", "Regon", "regon"])
    miasto = get_first(root, ["Miejscowosc", "Miejscowość", "Miasto", "gmina", "Powiat"])
    pkd    = get_first(root, ["PKD", "PKDGlowny", "PKD_PrzedmiotDzialalnosci"])
    ulica  = get_first(root, ["Ulica","ulica"])
    nrdom  = get_first(root, ["NrNieruchomosci","NumerNieruchomosci","NrDomu","nrdomu"])
    nrlok  = get_first(root, ["NrLokalu","NumerLokalu","nrlokalu"])
    kod    = get_first(root, ["KodPocztowy","Kod","kod"])
    miejsc = miasto or ""
    parts = [p for p in [ulica, nrdom, (("/" + nrlok) if nrlok else None), kod, miejsc] if p]
    adres = ", ".join([p for p in parts if p])

    return {
        "Nazwa_firmy": nazwa or f"Firma {nip}",
        "REGON": regon or "",
        "Miejscowość": miejsc,
        "PKD": pkd or "",
        "Adres": adres,
    }

# =========================
# Baza „lista wierzycieli” + osadzenie tabeli inline (tylko NIP, Adres jako dodatki)
# =========================
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
            "Adres": {"rich_text": {}},
        }
    )
    return db["id"]

def insert_creditor_rows(notion: Notion, db_id: str, rows: List[Dict[str, Any]]):
    def _rt(val: str) -> List[Dict[str, Any]]:
        return [{"type":"text","text":{"content": val}}] if val else []
    for r in rows:
        notion.pages.create(
            parent={"database_id": db_id},
            properties={
                "Kredytodawca": {"title":[{"type":"text","text":{"content": str(r.get("Kredytodawca",""))}}]},
                "Źródło": {"select":{"name": str(r.get("Źródło","auto")) or "auto"}},
                "Rodzaj_produktu": {"rich_text": _rt(str(r.get("Rodzaj_produktu","")))},
                "Zawarcie_umowy":  {"rich_text": _rt(str(r.get("Zawarcie_umowy","")))},
                "Pierwotna_kwota": {"number": _num_or_none(r.get("Pierwotna_kwota"))},
                "Pozostało_do_spłaty": {"number": _num_or_none(r.get("Pozostało_do_spłaty"))},
                "Kwota_raty": {"number": _num_or_none(r.get("Kwota_raty"))},
                "Suma_zaległości": {"number": _num_or_none(r.get("Suma_zaległości"))},
                "NIP": {"rich_text": _rt(str(r.get("NIP","")))},
                "Adres": {"rich_text": _rt(str(r.get("Adres","")))},
            }
        )

def embed_database_inline(notion: Notion, parent_page_id: str, database_id: str, heading: Optional[str] = None):
    blocks = []
    if heading:
        blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text":[{"type":"text","text":{"content": heading}}]}
        })
    blocks.append({
        "object": "block",
        "type": "link_to_page",
        "link_to_page": {"type": "database_id", "database_id": database_id}
    })
    notion.blocks.children.append(block_id=parent_page_id, children=blocks)

# =========================
# ROUTES
# =========================
@app.get("/")
def root():
    return {"ok": True, "service": "BIK PDF -> XLS", "expiring_days": LINK_TTL_DAYS}

@app.get("/poll-ui")
def poll_ui():
    html = """
    <!DOCTYPE html>
    <html lang="pl"><head><meta charset="utf-8"><title>Notion Poll</title></head>
    <body style="font-family:system-ui;max-width:700px;margin:40px auto">
      <h1>Wywołaj POST /notion/poll</h1>
      <button id="btn" style="padding:10px 16px;font-size:16px">Uruchom</button>
      <pre id="out" style="background:#f5f5f5;padding:12px;white-space:pre-wrap"></pre>
      <script>
        const btn = document.getElementById('btn');
        const out = document.getElementById('out');
        btn.onclick = async () => {
          btn.disabled = true; out.textContent = 'Wysyłam...';
          try {
            const r = await fetch('/notion/poll', {method:'POST'});
            const text = await r.text();
            try {
              const j = JSON.parse(text);
              out.textContent = JSON.stringify(j, null, 2);
            } catch(e) {
              out.textContent = 'HTTP ' + r.status + ' ' + r.statusText + '\\n\\n' + text;
            }
          } catch(e) { out.textContent = 'Błąd fetch: ' + e; }
          btn.disabled = false;
        };
      </script>
    </body></html>
    """
    return HTMLResponse(html)

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

@app.get("/notion/db-check")
def notion_db_check():
    try:
        notion = get_notion()
        db = notion.databases.retrieve(database_id=NOTION_DB_ID)
        title = ""
        if db.get("title"):
            title = "".join([t.get("plain_text","") for t in db["title"]])
        props = list(db.get("properties", {}).keys())
        return {"ok": True, "database_id": NOTION_DB_ID, "title": title, "properties": props}
    except APIResponseError as e:
        return JSONResponse({"ok": False, "where": "db-check", "code": getattr(e, "code", None), "message": str(e)}, status_code=500)
    except Exception as e:
        return JSONResponse({"ok": False, "where": "db-check", "message": str(e)}, status_code=500)

@app.post("/notion/poll")
async def notion_poll():
    try:
        notion = get_notion()
        # Tylko PDF is_not_empty + XLS is_empty
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
            pid = page["id"]
            props = page.get("properties", {})
            title = get_page_title_from_properties(props)
            pdf_prop = props.get(PROP_PDF, {})
            pdf_urls = extract_file_urls_from_notion_file_prop(pdf_prop)
            if not pdf_urls:
                continue

            all_rows: List[Dict[str, Any]] = []
            page_nip: Optional[str] = None
            page_pesel: Optional[str] = None
            company_data: Optional[Dict[str, Any]] = None

            for url in pdf_urls:
                try:
                    pdf_bytes = await http_get_bytes(url)
                    info = detect_source_and_ids(pdf_bytes)
                    src   = info["source"]
                    nip   = info["nip"]
                    pesel = info["pesel"]

                    parsed = parse_bik_pdf(pdf_bytes, source=src)

                    if nip and src == "firmowy":
                        if not company_data:
                            try:
                                company_data = await fetch_company_data_by_nip(nip)
                            except Exception as e:
                                print(f"[WARN] fetch_company_data_by_nip({nip}) failed: {e}")
                                company_data = None
                        for r in parsed:
                            r["NIP"] = nip
                            if company_data:
                                r["Adres"] = company_data.get("Adres","")
                        page_nip = page_nip or nip

                    if pesel and src == "prywatny":
                        page_pesel = page_pesel or pesel

                    all_rows.extend(parsed)
                except Exception as e:
                    print(f"[WARN] Pobieranie/parsowanie nie powiodło się dla {url}: {e}")

            if not all_rows:
                continue

            # XLS
            xlsx_bytes = rows_to_xlsx_bytes(all_rows)
            filename = _safe_name(f"{title}.xlsx")
            save_file(xlsx_bytes, filename)
            public_url = expiring_download_url(filename)

            # Baza „lista wierzycieli” + widok inline
            db_name = f"{title} lista wierzycieli"
            try:
                db_id = create_creditors_database(notion, parent_page_id=pid, db_name=db_name)
                insert_creditor_rows(notion, db_id, all_rows)
                embed_database_inline(notion, parent_page_id=pid, database_id=db_id, heading="Lista wierzycieli")
            except Exception as e:
                print(f"[WARN] Nie udało się utworzyć/wstawić bazy wierzycieli dla {title}: {e}")

            # Link do XLS
            notion.pages.update(page_id=pid, properties={PROP_XLS: {"url": public_url}})

            # Uzupełnij dane w bazie Klientów (tylko jeśli puste)
            page_after = notion.pages.retrieve(page_id=pid)
            props_after = page_after.get("properties", {})
            if page_nip:
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_NIP_MAIN, page_nip)
            if page_pesel:
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_PESEL_MAIN, page_pesel)
            if company_data:
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_FIRMA_NAME, company_data.get("Nazwa_firmy",""))
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_REGON,       company_data.get("REGON",""))
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_CITY,        company_data.get("Miejscowość",""))
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_PKD,         company_data.get("PKD",""))
                set_page_text_prop_if_empty(notion, pid, props_after, PROP_ADDRESS,     company_data.get("Adres",""))

            processed += 1

        return {"ok": True, "processed": processed}
    except APIResponseError as e:
        return JSONResponse({"ok": False, "where": "notion", "code": getattr(e, "code", None), "message": str(e)}, status_code=500)
    except Exception as e:
        return JSONResponse({"ok": False, "where": "server", "message": str(e)}, status_code=500)

# ===== ENRICH (uzupełnianie po NIP – tylko baza Klientów) =====
ENRICH_SECRET   = os.getenv("ENRICH_SECRET", "")

def _first_empty(fields: list[str], props: Dict[str, Any]) -> bool:
    for fn in fields:
        p = props.get(fn, {})
        t = p.get("type")
        if t == "rich_text" and len(p.get("rich_text", [])) == 0: return True
        if t == "title" and len(p.get("title", [])) == 0: return True
        if t == "number" and p.get("number") is None: return True
        if t == "select" and p.get("select") is None: return True
        if t == "multi_select" and len(p.get("multi_select", [])) == 0: return True
        if t == "url" and not p.get("url"): return True
        if t == "date" and not p.get("date"): return True
        if t is None:
            return True
    return False

def _set_text(prop_name: str, value: str) -> Dict[str, Any]:
    return {prop_name: {"rich_text": [{"type": "text", "text": {"content": value}}]}}

@app.post("/notion/enrich")
async def notion_enrich(x_enrich_key: str | None = Query(default=None)):
    if ENRICH_SECRET and x_enrich_key != ENRICH_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    notion = get_notion()
    resp = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={"property": PROP_NIP_MAIN, "rich_text": {"is_not_empty": True}},
        page_size=50
    )
    results = resp.get("results", [])
    updated = 0

    for page in results:
        pid = page["id"]
        props = page.get("properties", {})
        nip_prop = props.get(PROP_NIP_MAIN, {})
        nip_text = ""
        if nip_prop.get("type") == "rich_text":
            nip_text = "".join([x.get("plain_text","") for x in nip_prop.get("rich_text", [])]).strip()
        elif nip_prop.get("type") == "title":
            nip_text = "".join([x.get("plain_text","") for x in nip_prop.get("title", [])]).strip()
        elif nip_prop.get("type") == "number":
            nip_text = str(nip_prop.get("number") or "")

        if not nip_text:
            continue

        # Czy mamy braki do uzupełnienia?
        if not _first_empty([PROP_FIRMA_NAME, PROP_REGON, PROP_CITY, PROP_PKD, PROP_ADDRESS], props):
            continue

        try:
            data = await fetch_company_data_by_nip(nip_text)
        except Exception as e:
            print(f"[WARN] enrich nip {nip_text} failed: {e}")
            continue

        patch: Dict[str, Any] = {}
        if props.get(PROP_FIRMA_NAME) and _first_empty([PROP_FIRMA_NAME], props) and data.get("Nazwa_firmy"):
            patch.update(_set_text(PROP_FIRMA_NAME, data["Nazwa_firmy"]))
        if props.get(PROP_REGON) and _first_empty([PROP_REGON], props) and data.get("REGON"):
            patch.update(_set_text(PROP_REGON, data["REGON"]))
        if props.get(PROP_CITY) and _first_empty([PROP_CITY], props) and data.get("Miejscowość"):
            patch.update(_set_text(PROP_CITY, data["Miejscowość"]))
        if props.get(PROP_PKD) and _first_empty([PROP_PKD], props) and data.get("PKD"):
            patch.update(_set_text(PROP_PKD, data["PKD"]))
        if props.get(PROP_ADDRESS) and _first_empty([PROP_ADDRESS], props) and data.get("Adres"):
            patch.update(_set_text(PROP_ADDRESS, data["Adres"]))

        if patch:
            notion.pages.update(page_id=pid, properties=patch)
            updated += 1

    return {"ok": True, "updated": updated}
