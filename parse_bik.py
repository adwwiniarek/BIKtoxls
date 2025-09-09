# parse_bik.py
import re
import fitz  # PyMuPDF
from typing import List, Dict, Any, Optional

UPPER_RE = re.compile(r"^[^a-ząćęłńóśżź]+$")  # linia bez małych – traktujemy jako WIERZYCIEL (CAPS)
SPACES_RE = re.compile(r"\s+")
NUM_RE = re.compile(r"[-+]?[0-9][0-9\s]*([.,][0-9]{1,2})?")

HEAD_IN_PROGRESS = "Zobowiązania finansowe - w trakcie spłaty"

def _clean_line(s: str) -> str:
    s = s.replace("\t", " ")
    s = SPACES_RE.sub(" ", s)
    s = s.strip().strip('"').strip("'")
    return s

def _num(s: Optional[str]) -> Optional[float]:
    if not s: return None
    s = s.replace(" ", "").replace("\u00A0","").replace("PLN","").strip()
    if s.upper() == "ND": return None
    if s.upper() == "BRAK": return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        m = NUM_RE.search(s)
        if m:
            try:
                return float(m.group(0).replace(" ", "").replace(",", "."))
            except:
                return None
        return None

def _extract_table_rows(text_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    in_section = False
    last_product: Optional[str] = None

    for i, raw in enumerate(text_lines):
        line = _clean_line(raw)

        if not in_section:
            if HEAD_IN_PROGRESS.lower() in line.lower():
                in_section = True
            continue

        if not line:
            continue

        # wierzyciel – linia CAPS
        if UPPER_RE.match(line) and len(line) > 3:
            creditor = line
            product = last_product or ""
            last_product = None

            z = {
                "Rodzaj_produktu": product,
                "Kredytodawca": creditor,
                "Zawarcie_umowy": "",
                "Pierwotna_kwota": None,
                "Pozostało_do_spłaty": None,
                "Kwota_raty": None,
                "Suma_zaległości": None,
            }

            # spójrzmy 1–3 linii do przodu po datę + kwoty
            for j in range(1, 4):
                if i + j >= len(text_lines): break
                nxt = _clean_line(text_lines[i + j])
                if not nxt: continue

                mdate = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", nxt)
                if mdate and not z["Zawarcie_umowy"]:
                    z["Zawarcie_umowy"] = mdate.group(0)

                nums = [x.group(0) for x in NUM_RE.finditer(nxt)]
                if nums:
                    if z["Pierwotna_kwota"] is None and len(nums) >= 1:
                        z["Pierwotna_kwota"] = _num(nums[0])
                    if z["Pozostało_do_spłaty"] is None and len(nums) >= 2:
                        z["Pozostało_do_spłaty"] = _num(nums[1])
                    if z["Kwota_raty"] is None and len(nums) >= 3:
                        z["Kwota_raty"] = _num(nums[2])
                    if z["Suma_zaległości"] is None and len(nums) >= 4:
                        z["Suma_zaległości"] = _num(nums[3])

                done_core = (
                    z["Zawarcie_umowy"] and
                    (z["Pierwotna_kwota"] is not None or z["Pozostało_do_spłaty"] is not None)
                )
                if done_core:
                    break

            if z["Kwota_raty"] is None:
                z["Kwota_raty"] = None
            if z["Suma_zaległości"] is None:
                z["Suma_zaległości"] = 0.0

            rows.append(z)
            continue

        # kandydat na Rodzaj_produktu – linia nie-CAPS, omijamy nagłówki/legendy
        if not UPPER_RE.match(line):
            if not any(k.lower() in line.lower() for k in [
                "zawarcie", "pierwotna", "pozostało", "kwota raty",
                "suma zaległości", "historia", "ostatnia płatność", "łącznie"
            ]):
                last_product = line

    return rows

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines: List[str] = []
    for p in doc:
        try:
            t = p.get_text("text") or ""
        except:
            t = ""
        if t:
            lines.extend([ln for ln in t.splitlines()])
    doc.close()

    rows = _extract_table_rows(lines)

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "Źródło": source,
            "Rodzaj_produktu": r.get("Rodzaj_produktu","") or "",
            "Kredytodawca": r.get("Kredytodawca","") or "",
            "Zawarcie_umowy": r.get("Zawarcie_umowy","") or "",
            "Pierwotna_kwota": r.get("Pierwotna_kwota"),
            "Pozostało_do_spłaty": r.get("Pozostało_do_spłaty"),
            "Kwota_raty": r.get("Kwota_raty"),
            "Suma_zaległości": r.get("Suma_zaległości", 0.0),
            "NIP": "",
            "Adres": "",
        })
    return out
