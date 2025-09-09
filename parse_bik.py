# parse_bik.py  — stabilny, z łączeniem wielowierszowych nazw
import re
import fitz  # PyMuPDF
from typing import List, Dict, Any, Optional

UPPER_RE = re.compile(r"^[^a-ząćęłńóśżź]+$")  # cała linia bez małych (CAPS)
SPACES_RE = re.compile(r"\s+")
NUM_RE = re.compile(r"[-+]?[0-9][0-9\s]*([.,][0-9]{1,2})?")

HEAD_IN_PROGRESS = "Zobowiązania finansowe - w trakcie spłaty"

def _clean_line(s: str) -> str:
    s = s.replace("\t", " ")
    s = SPACES_RE.sub(" ", s)
    return s.strip().strip('"').strip("'")

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
            try: return float(m.group(0).replace(" ", "").replace(",", "."))
            except: return None
        return None

def _join_caps(lines: List[str], start_idx: int) -> (str, int):
    """Sklej kilka kolejnych wierszy CAPS w jedną nazwę; zwróć (nazwa, last_idx)."""
    parts = []
    i = start_idx
    while i < len(lines):
        ln = _clean_line(lines[i])
        if not ln or not UPPER_RE.match(ln): break
        parts.append(ln)
        # Jeśli linia wygląda na urwaną (np. kończy się pojedynczą literą), i kolejna też CAPS — doklej
        i += 1
    name = " ".join(parts)
    return name, i-1

def _join_product(lines: List[str], start_idx: int) -> (str, int):
    """Sklej produkt z kolejnych wierszy nie-CAPS (do pierwszego CAPS/kolumny)."""
    parts = []
    i = start_idx
    while i < len(lines):
        ln = _clean_line(lines[i])
        if not ln or UPPER_RE.match(ln): break
        # pomijamy nagłówki tabeli
        low = ln.lower()
        if any(k in low for k in ["zawarcie", "pierwotna", "pozostało", "kwota raty", "suma zaległości", "historia", "ostatnia płatność", "łącznie", "zamknięte"]):
            break
        parts.append(ln)
        i += 1
    prod = " ".join(parts)
    return prod.strip(), i-1

def _extract_table_rows(text_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    in_section = False

    i = 0
    n = len(text_lines)
    while i < n:
        line = _clean_line(text_lines[i])

        if not in_section:
            if HEAD_IN_PROGRESS.lower() in line.lower():
                in_section = True
            i += 1
            continue

        if not line:
            i += 1
            continue

        # wyjście z sekcji
        low = line.lower()
        if low.startswith("łącznie") or "zamknięte" in low:
            break

        # produkt (nie-CAPS) → może zajmować kilka wierszy
        if not UPPER_RE.match(line):
            product, i = _join_product(text_lines, i)
            # po produkcie oczekujemy CAPS (wierzyciel)
            i += 1
            if i >= n: break
            nxt = _clean_line(text_lines[i])
            if not nxt or not UPPER_RE.match(nxt):
                # jeśli jednak nie ma wierzyciela, przejdź dalej
                continue
            # wierzyciel wielowierszowy
            creditor, i = _join_caps(text_lines, i)

            # teraz poszukaj daty + liczb w kolejnych 1–4 liniach
            z = {
                "Rodzaj_produktu": product,
                "Kredytodawca": creditor,
                "Zawarcie_umowy": "",
                "Pierwotna_kwota": None,
                "Pozostało_do_spłaty": None,
                "Kwota_raty": None,
                "Suma_zaległości": None,
            }
            for j in range(1, 5):
                if i + j >= n: break
                nxt2 = _clean_line(text_lines[i + j])
                if not nxt2: continue
                mdate = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", nxt2)
                if mdate and not z["Zawarcie_umowy"]:
                    z["Zawarcie_umowy"] = mdate.group(0)
                nums = [m.group(0) for m in NUM_RE.finditer(nxt2)]
                if nums:
                    if z["Pierwotna_kwota"] is None and len(nums) >= 1:
                        z["Pierwotna_kwota"] = _num(nums[0])
                    if z["Pozostało_do_spłaty"] is None and len(nums) >= 2:
                        z["Pozostało_do_spłaty"] = _num(nums[1])
                    if z["Kwota_raty"] is None and len(nums) >= 3:
                        z["Kwota_raty"] = _num(nums[2])
                    if z["Suma_zaległości"] is None and len(nums) >= 4:
                        z["Suma_zaległości"] = _num(nums[3])

            if z["Kwota_raty"] is None:
                z["Kwota_raty"] = None
            if z["Suma_zaległości"] is None:
                z["Suma_zaległości"] = 0.0

            rows.append(z)
            # przeskocz kilka linii z liczbami
            i += 4
            continue

        # jeśli trafimy CAPS bez produktu (rzadkie w tej sekcji) – zignoruj
        i += 1

    return rows

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines: List[str] = []
    for p in doc:
        t = p.get_text("text") or ""
        if t: lines.extend(t.splitlines())
    doc.close()

    rows = _extract_table_rows(lines)

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "Źródło": source,
            "Rodzaj_produktu": r.get("Rodzaj_produktu",""),
            "Kredytodawca": r.get("Kredytodawca",""),
            "Zawarcie_umowy": r.get("Zawarcie_umowy",""),
            "Pierwotna_kwota": r.get("Pierwotna_kwota"),
            "Pozostało_do_spłaty": r.get("Pozostało_do_spłaty"),
            "Kwota_raty": r.get("Kwota_raty"),
            "Suma_zaległości": r.get("Suma_zaległości", 0.0),
            "NIP": "",
            "Adres": "",
        })
    return out
