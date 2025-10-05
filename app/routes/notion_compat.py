from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse
import io, time
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import httpx

from app.notion_client import notion
from app.config import X_API_KEY
from services.bik_parser import parse_bik_pdf

router = APIRouter()

def _stable_download_url(request: Request, page_id: str, x_key: str) -> str:
    base = str(request.url.include_query_params(page_id=page_id, x_key=x_key))
    parts = list(urlparse(base))
    q = [(k, v) for k, v in parse_qsl(parts[4]) if k not in ("debug", "write", "embed", "enrich")]
    q.append(("v", str(int(time.time()))))
    parts[4] = urlencode(q)
    return urlunparse(parts)

def _norm_name(s: str) -> str:
    return (s or "").strip().lower().replace("  ", " ")

def _find_files_property(props: dict) -> tuple[str, dict] | tuple[None, None]:
    # Preferuj dokładnie "Raporty BIK"
    for k, v in props.items():
        if k == "Raporty BIK" and v.get("type") == "files":
            return k, v
    # Inne nazwy, jeśli ktoś zmienił (szukamy typu files z „raport” w nazwie)
    for k, v in props.items():
        if v.get("type") == "files" and "raport" in _norm_name(k):
            return k, v
    return None, None

def _to_iso_date(d: str | None):
    if not d: return None
    try:
        dd, mm, yyyy = d.split(".")
        return f"{yyyy}-{mm}-{dd}"
    except:
        return None

async def _ensure_child_database(page_id: str) -> str:
    title = "Wiersze BIK"
    db = await notion.databases.create(
        parent={"type": "page_id", "page_id": page_id},
        title=[{"type": "text", "text": {"content": title}}],
        properties={
            "Źródło": {"type": "select", "select": {}},
            "Rodzaj_produktu": {"type": "rich_text"},
            "Kredytodawca": {"type": "rich_text"},
            "Zawarcie_umowy": {"type": "date"},
            "Pierwotna_kwota": {"type": "number", "number": {"format": "zloty"}},
            "Pozostało_do_spłaty": {"type": "number", "number": {"format": "zloty"}},
            "Kwota_raty": {"type": "number", "number": {"format": "zloty"}},
            "Suma_zaległości": {"type": "number", "number": {"format": "zloty"}},
        }
    )
    return db["id"]

async def _insert_rows(db_id: str, rows: list[dict]):
    for r in rows:
        props = {
            "Źródło": {"select": {"name": (r.get("Źródło") or "PDF")[:100]}},
            "Rodzaj_produktu": {"rich_text": [{"type": "text", "text": {"content": r.get("Rodzaj_produktu","")[:2000]}}]},
            "Kredytodawca": {"rich_text": [{"type": "text", "text": {"content": r.get("Kredytodawca","")[:2000]}}]},
            "Zawarcie_umowy": {"date": {"start": _to_iso_date(r.get("Zawarcie_umowy"))}},
            "Pierwotna_kwota": {"number": r.get("Pierwotna_kwota")},
            "Pozostało_do_spłaty": {"number": r.get("Pozostało_do_spłaty")},
            "Kwota_raty": {"number": r.get("Kwota_raty")},
            "Suma_zaległości": {"number": r.get("Suma_zaległości")},
        }
        await notion.pages.create(parent={"type": "database_id", "database_id": db_id}, properties=props)

async def _enrich_from_nip(page: dict) -> dict:
    props = page.get("properties", {})
    nip = ""
    # znajdź property NIP (rich_text lub number)
    for key, p in props.items():
        if _norm_name(key) == "nip":
            if p["type"] == "rich_text" and p["rich_text"]:
                nip = "".join(c for c in p["rich_text"][0]["plain_text"] if c.isdigit())
            elif p["type"] == "number" and p.get("number"):
                nip = str(p["number"])
            break
    if not nip:
        return {}
    url = f"https://api.officeblog.pl/gus.php?NIP={nip}&format=0"
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return {}
    updates = {}
    if data.get("regon"):
        updates["REGON"] = {"rich_text": [{"type":"text","text":{"content": data["regon"]}}]}
    if data.get("miejscowosc"):
        updates["Miejscowość"] = {"rich_text": [{"type":"text","text":{"content": data["miejscowosc"]}}]}
    if data.get("adres"):
        updates["Adres"] = {"rich_text": [{"type":"text","text":{"content": data["adres"]}}]}
    if data.get("nazwa"):
        updates["Nazwa"] = {"rich_text": [{"type":"text","text":{"content": data["nazwa"]}}]}
    return updates

@router.get("/notion/poll-one")
async def poll_one(
    request: Request,
    page_id: str = Query(...),
    x_key: str = Query(...),
    write: int = 0,
    embed: int = 0,
    enrich: int = 0,
    debug: int = 0,
):
    # opcjonalne sprawdzenie klucza API
    if X_API_KEY and x_key != X_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    # 1) pobierz stronę i property z PDF-ami
    page = await notion.pages.retrieve(page_id=page_id)
    props = page.get("properties", {})
    files_key, files_prop = _find_files_property(props)
    if not files_prop:
        raise HTTPException(400, detail="Property 'Raporty BIK' (Files) nie istnieje na stronie.")

    pdf_urls = []
    for f in files_prop["files"]:
        if f["type"] == "file":
            url = f["file"].get("url","")
        else:
            url = f["external"].get("url","")
        if url.lower().endswith(".pdf"):
            pdf_urls.append(url)

    if not pdf_urls:
        return JSONResponse({"detail": "Nie znaleziono PDF-ów w 'Raporty BIK'."}, status_code=404)

    # 2) pobierz PDF-y i parsuj
    all_rows: list[dict] = []
    async with httpx.AsyncClient(timeout=40) as client:
        for u in pdf_urls:
            res = await client.get(u)
            res.raise_for_status()
            all_rows.extend(parse_bik_pdf(res.content, source="BIK PDF"))

    if debug:
        return {"ok": True, "pdfs": len(pdf_urls), "rows": len(all_rows)}

    # 3) zbuduj XLS
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK"
    header = ["Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
              "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości"]
    ws.append(header)
    for r in all_rows:
        ws.append([r.get(k) for k in header])
    bio = io.BytesIO()
    wb.save(bio)
    xls_bytes = bio.getvalue()

    # 4) zapis do Notion (opcjonalnie wg flag)
    if write or embed or enrich:
        updates = {}
        download_url = _stable_download_url(request, page_id, x_key)

        if write:
            updates["XLS"] = {"url": download_url}
            if "gotowe kalkulacje" in props and props["gotowe kalkulacje"]["type"] == "files":
                updates["gotowe kalkulacje"] = {
                    "files": [{
                        "name": f"bik_{page_id}.xlsx",
                        "external": {"url": download_url}
                    }]
                }
            updates["Status"] = {"select": {"name": "Przetworzony BIK na XlS"}}

        if enrich:
            enrich_updates = await _enrich_from_nip(page)
            updates.update(enrich_updates)

        if updates:
            await notion.pages.update(page_id=page_id, properties=updates)

        if embed:
            db_id = await _ensure_child_database(page_id)
            await _insert_rows(db_id, all_rows)

    # 5) zwróć XLS (auto-pobieranie w przeglądarce zostaje)
    return StreamingResponse(
        io.BytesIO(xls_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="bik_{page_id}.xlsx"'}
    )
