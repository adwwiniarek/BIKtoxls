from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any
from services.bik_parser import parse_bik_pdf

router = APIRouter()

def _build_xls(rows: List[Dict[str, Any]], filename: str = "BIK.xlsx") -> StreamingResponse:
    from openpyxl import Workbook
    from io import BytesIO
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK"
    headers = [
        "Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
        "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości",
    ]
    ws.append(headers)
    for r in rows:
        ws.append([
            r.get("Źródło"), r.get("Rodzaj_produktu"), r.get("Kredytodawca"),
            r.get("Zawarcie_umowy"), r.get("Pierwotna_kwota"),
            r.get("Pozostało_do_spłaty"), r.get("Kwota_raty"),
            r.get("Suma_zaległości"),
        ])
    from io import BytesIO
    buf = BytesIO(); wb.save(buf); buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@router.post("/bik/pdf-to-xls")
async def pdf_to_xls(files: List[UploadFile] = File(...)):
    rows_all: List[Dict[str, Any]] = []
    for f in files:
        data = await f.read()
        rows_all.extend(parse_bik_pdf(data, source=f.filename))
    if not rows_all:
        raise HTTPException(status_code=422, detail="Nie udało się wyciągnąć danych z PDF.")
    fname = "BIK_z_raportow.xlsx" if len(files) > 1 else "BIK_z_raportu.xlsx"
    return _build_xls(rows_all, filename=fname)
