from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from parse_bik import parse_bik_pdf
from openpyxl import Workbook
import io

app = FastAPI(title="BIK PDF -> XLS")

def rows_to_xlsx_bytes(rows: list[dict]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "BIK_Raport"
    headers = ["Źródło","Rodzaj_produktu","Kredytodawca","Zawarcie_umowy",
               "Pierwotna_kwota","Pozostało_do_spłaty","Kwota_raty","Suma_zaległości"]
    ws.append(headers)
    for r in rows:
        ws.append([r.get(h, "") for h in headers])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()

@app.get("/")
def root():
    return {"ok": True, "service": "BIK PDF -> XLS"}

@app.post("/parse")
async def parse_endpoint(file: UploadFile = File(...), source_label: str = "auto"):
    if (file.content_type or "").lower() not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(status_code=400, detail="Prześlij plik PDF")
    pdf_bytes = await file.read()
    rows = parse_bik_pdf(pdf_bytes, source_label)
    if not rows:
        return JSONResponse({"ok": False, "rows": 0, "msg": "Brak danych w sekcji 'w trakcie spłaty'."}, status_code=422)
    xlsx = rows_to_xlsx_bytes(rows)
    return StreamingResponse(
        io.BytesIO(xlsx),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="bik.xlsx"'}
    )
