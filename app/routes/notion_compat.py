# routes/notion_compat.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
import os
from typing import Optional, List, Dict, Any
import httpx
import fitz  # PyMuPDF
from services.bik_parser import parse_bik_pdf
from app.notion_client import NotionClient, NotionError  # mamy app/… więc tak importujemy

router = APIRouter()

def _fetch_url(url: str) -> bytes:
    with httpx.Client(timeout=60) as cli:
        r = cli.get(url, follow_redirects=True)
        r.raise_for_status()
        return r.content

def _filename_from_url(url: str) -> str:
    try:
        return url.split("?")[0].split("/")[-1] or "plik.pdf"
    except Exception:
        return "plik.pdf"

def _bik_pdfs_from_notion(page_id: str) -> List[tuple[str, bytes]]:
    """
    Pobiera pliki z właściwości **Raporty BIK** (typ Files) na stronie Notion.
    Zwraca listę (nazwa, bytes).
    """
    client = NotionClient()
    page = client.get_page(page_id)
    props = page.get("properties", {})
    field = props.get("Raporty BIK")
    if not field or field.get("type") != "files":
        raise HTTPException(status_code=404, detail="Nie znaleziono PDF-ów w 'Raporty BIK'.")

    files = field.get("files", []) or []
    out: List[tuple[str, bytes]] = []
    for f in files:
        name = f.get("name") or "raport.pdf"
        ftype = f.get("type")
        if ftype == "file":
            url = f["file"]["url"]
        elif ftype == "external":
            url = f["external"]["url"]
        else:
            continue
        # tylko PDF-y
        if not (name.lower().endswith(".pdf") or url.lower().endswith(".pdf")):
            continue
        out.append((name, _fetch_url(url)))

    if not out:
        raise HTTPException(status_code=404, detail="Nie znaleziono PDF-ów w 'Raporty BIK'.")
    return out

def _build_xls(rows: List[Dict[str, Any]], filename: str = "BIK.xlsx") -> StreamingResponse:
    """
    Minimalny builder XLS (openpyxl) – jedna zakładka, scalone wszystkie wiersze.
    """
    from openpyxl import Workbook
    from io import BytesIO

    wb = Workbook()
    ws = wb.active
    ws.title = "BIK"

    headers = [
        "Źródło",
        "Rodzaj_produktu",
        "Kredytodawca",
        "Zawarcie_umowy",
        "Pierwotna_kwota",
        "Pozostało_do_spłaty",
        "Kwota_raty",
        "Suma_zaległości",
    ]
    ws.append(headers)

    for r in rows:
        ws.append([
            r.get("Źródło"),
            r.get("Rodzaj_produktu"),
            r.get("Kredytodawca"),
            r.get("Zawarcie_umowy"),
            r.get("Pierwotna_kwota"),
            r.get("Pozostało_do_spłaty"),
            r.get("Kwota_raty"),
            r.get("Suma_zaległości"),
        ])

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@router.get("/notion/poll-one")
def notion_poll_one(
    page_id: str = Query(...),
    x_key: Optional[str] = Query(None),
    debug: Optional[int] = Query(None),
):
    # prosty „sekret” kompatybilny ze starymi wywołaniami
    required = os.getenv("NOTION_X_KEY")
    if required and x_key != required:
        raise HTTPException(status_code=403, detail="Forbidden")

    rows_all: List[Dict[str, Any]] = []
    pdfs = _bik_pdfs_from_notion(page_id)

    dbg: Dict[str, Any] = {"pdfs": len(pdfs), "rows": 0, "notes": []}
    for name, pdf_bytes in pdfs:
        # notatka diagnostyczna: długość tekstu z 1. strony
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            t = doc[0].get_text("text") if len(doc) > 0 else ""
            dbg["notes"].append({"file": name, "p1_len": len(t or "")})
        except Exception:
            dbg["notes"].append({"file": name, "p1_len": None})

        rows = parse_bik_pdf(pdf_bytes, source=name)
        rows_all.extend(rows)

    dbg["rows"] = len(rows_all)

    if debug:
        return JSONResponse(dbg, 200)

    fname = "BIK_z_raportow.xlsx" if len(pdfs) > 1 else "BIK_z_raportu.xlsx"
    return _build_xls(rows_all, filename=fname)
