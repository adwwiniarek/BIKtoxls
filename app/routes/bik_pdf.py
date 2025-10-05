# app/routes/bik_pdf.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import StreamingResponse
from io import BytesIO
import httpx, os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from ..notion_client import NotionClient, NotionError
from services.bik_parser import parse_bik_pdf  # <— Twój parser

router = APIRouter()

# ---------- helpers: fetch ----------
def _fetch_url(url: str) -> bytes:
    try:
        with httpx.Client(timeout=60) as cli:
            r = cli.get(url, follow_redirects=True)
            r.raise_for_status()
            return r.content
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Nie można pobrać PDF: {e}")

def _filename_from_url(url: str) -> str:
    try:
        path = urlparse(url).path or ""
        name = os.path.basename(path) or "raport.pdf"
        return name
    except Exception:
        return "raport.pdf"

# ---------- Notion: list all PDFs from 'Raporty BIK' ----------
def _bik_pdfs_from_notion(page_id: str) -> List[Tuple[str, bytes]]:
    """
    Zwraca LISTĘ (nazwa, bytes) wszystkich PDF-ów z właściwości 'Raporty BIK'.
    Jeśli brak PDF-ów -> 404.
    """
    client = NotionClient()
    try:
        page = client.get_page(page_id)
    except NotionError as e:
        raise HTTPException(status_code=502, detail=f"Notion error: {e}")

    props = page.get("properties", {}) or {}
    files = (props.get("Raporty BIK") or {}).get("files", []) or []
    out: List[Tuple[str, bytes]] = []

    for f in files:
        nm = f.get("name") or "raport.pdf"
        if f.get("type") == "file":
            url = (f.get("file") or {}).get("url")
        else:
            url = (f.get("external") or {}).get("url")
        if url and url.lower().endswith(".pdf"):
            out.append((nm, _fetch_url(url)))

    if not out:
        raise HTTPException(status_code=404, detail="Nie znaleziono PDF-ów w 'Raporty BIK'.")
    return out

# ---------- XLS builder (nowe nagłówki) ----------
HEADERS = [
    "Źródło",
    "Rodzaj_produktu",
    "Kredytodawca",
    "Zawarcie_umowy",
    "Pierwotna_kwota",
    "Pozostało_do_spłaty",
    "Kwota_raty",
    "Suma_zaległości",
]

def _build_xls(rows: List[Dict[str, Any]], filename: str) -> StreamingResponse:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment

    wb = Workbook()
    ws = wb.active
    ws.title = "Zobowiązania (BIK)"

    # header
    for ci, h in enumerate(HEADERS, start=1):
        c = ws.cell(row=1, column=ci, value=h)
        c.font = Font(bold=True)
        c.alignment = Alignment(horizontal="center")

    # rows (może być 0 – wtedy dostajesz pusty szablon z nagłówkami)
    rr = 2
    for row in rows:
        ws.cell(row=rr, column=1, value=row.get("Źródło", ""))
        ws.cell(row=rr, column=2, value=row.get("Rodzaj_produktu", ""))
        ws.cell(row=rr, column=3, value=row.get("Kredytodawca", ""))
        ws.cell(row=rr, column=4, value=row.get("Zawarcie_umowy", ""))
        ws.cell(row=rr, column=5, value=row.get("Pierwotna_kwota"))
        ws.cell(row=rr, column=6, value=row.get("Pozostało_do_spłaty"))
        ws.cell(row=rr, column=7, value=row.get("Kwota_raty"))
        ws.cell(row=rr, column=8, value=row.get("Suma_zaległości"))
        rr += 1

    # widths + wrap
    widths = [26, 22, 40, 18, 18, 20, 16, 18]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[chr(ord("A") + i - 1)].width = w
    for r in ws.iter_rows(min_row=2, min_col=1, max_col=len(HEADERS), max_row=max(rr-1, 2)):
        for cell in r:
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ---------- POST /bik/pdf-to-xls (scala wiele PDF-ów z Notion) ----------
@router.post("/bik/pdf-to-xls")
async def bik_pdf_to_xls(
    pdf_url: Optional[str] = Form(None, description="URL do jednego PDF raportu BIK"),
    notion_page_id: Optional[str] = Form(None, description="Notion page_id — zbierze WSZYSTKIE PDF-y z 'Raporty BIK'"),
    file: Optional[UploadFile] = File(None, description="Lub bezpośredni upload jednego PDF")
):
    rows_all: List[Dict[str, Any]] = []

    # 1) Notion: zbierz wszystkie PDF-y i sklej wyniki
    if notion_page_id:
        pdfs = _bik_pdfs_from_notion(notion_page_id)  # lista (nazwa, bytes)
        for name, pdf_bytes in pdfs:
            rows_all.extend(parse_bik_pdf(pdf_bytes, source=name))
        fname = "BIK_z_raportow.xlsx" if len(pdfs) > 1 else "BIK_z_raportu.xlsx"
        return _build_xls(rows_all, filename=fname)

    # 2) Upload jednego PDF-a
    if file is not None:
        pdf_bytes = await file.read()
        rows_all = parse_bik_pdf(pdf_bytes, source="upload.pdf")
        return _build_xls(rows_all, filename="BIK_z_raportu.xlsx")

    # 3) Jeden PDF z URL
    if pdf_url:
        pdf_bytes = _fetch_url(pdf_url)
        rows_all = parse_bik_pdf(pdf_bytes, source=_filename_from_url(pdf_url))
        return _build_xls(rows_all, filename="BIK_z_raportu.xlsx")

    raise HTTPException(status_code=422, detail="Podaj PDF (plik), albo pdf_url, albo notion_page_id.")

# ---------- GET /notion/poll-one (kompatybilność; scala wiele PDF-ów) ----------
@router.get("/notion/poll-one")
def notion_poll_one_compat(page_id: str = Query(...), x_key: str | None = Query(None)):
    """
    GET /notion/poll-one?page_id=...&x_key=...
    Zwraca JEDEN XLS, który łączy WSZYSTKIE PDF-y z 'Raporty BIK' dla danej strony Notion.
    (Tylko dane z PDF, bez wzbogacania po NIP.)
    """
    required = os.getenv("NOTION_X_KEY")
    if required and x_key != required:
        raise HTTPException(status_code=403, detail="Forbidden")

    rows_all: List[Dict[str, Any]] = []
    pdfs = _bik_pdfs_from_notion(page_id)  # lista (nazwa, bytes)
    for name, pdf_bytes in pdfs:
        rows_all.extend(parse_bik_pdf(pdf_bytes, source=name))

    fname = "BIK_z_raportow.xlsx" if len(pdfs) > 1 else "BIK_z_raportu.xlsx"
    return _build_xls(rows_all, filename=fname)
