import fitz
import re
import unicodedata
from typing import List, Dict, Any, Optional

KNOWN_PRODUCTS = frozenset([
    "Bezumowny debet w koncie", "Karta kredytowa", "Kredyt gotówkowy / pożyczka gotówkowa",
    "Kredyt gotówkowy, pożyczka bankowa", "Kredyt mieszkaniowy", "Kredyt na zakup towarów i usług",
    "Kredyt obrotowy", "Kredyt odnawialny", "Kredyt w rachunku bieżącym lub limit debetowy", "Pożyczka"
])

RE_SECTION_START = re.compile(r"Zobowiązania finansowe - w trakcie spłaty", re.I)
RE_SECTION_END = re.compile(r"^(Łącznie|Zobowiązania finansowe - zamknięte|Informacje dodatkowe|Informacje szczegółowe)", re.I)
RE_DATE_START = re.compile(r"^\d{2}\.\d{2}\.\d{4}")
RE_LENDER = re.compile(r"^[A-ZĄĆĘŁŃÓŚŹŻ\s\d\.\-/]+$")
AMOUNT_RE = re.compile(r"\b(ND|BRAK|(?:\d{1,3}(?:\s\d{3})*|\d+)(?:,\d{2})?)\b(?:\s*PLN)?", re.I)


def _deep_clean_text(text: str) -> str:
    text = unicodedata.normalize('NFKD', text)
    return re.sub(r'\s+', ' ', text).strip()

def _read_lines_from_pdf(pdf_bytes: bytes) -> List[str]:
    lines = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            blocks = page.get_text("blocks", sort=True)
            for b in blocks:
                block_text = b[4]
                for line in block_text.splitlines():
                    cleaned_line = _deep_clean_text(line)
                    if cleaned_line:
                        lines.append(cleaned_line)
    return lines

def _slice_active_debt_section(all_lines: List[str]) -> List[str]:
    try:
        start_index = next(i for i, line in enumerate(all_lines) if RE_SECTION_START.search(line))
        end_index = len(all_lines)
        for i in range(start_index + 1, len(all_lines)):
            if RE_SECTION_END.search(all_lines[i]):
                end_index = i
                break
        return all_lines[start_index + 1:end_index]
    except StopIteration:
        return []

def _parse_amount(token: Optional[str]) -> Optional[float]:
    if token is None: return None
    token_upper = token.upper()
    if "ND" in token_upper: return None
    if "BRAK" in token_upper: return 0.0
    
    cleaned_token = token.replace("PLN", "").strip()
    cleaned_token = re.sub(r'\s', '', cleaned_token)
    cleaned_token = cleaned_token.replace(",", ".")
    
    try:
        return float(cleaned_token)
    except (ValueError, TypeError):
        return None

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    all_lines = _read_lines_from_pdf(pdf_bytes)
    debt_lines = _slice_active_debt_section(all_lines)

    if not debt_lines: return []

    final_records: List[Dict[str, Any]] = []
    state = "LOOKING_FOR_LENDER"
    current_record_data: Dict[str, List[str]] = {}

    def finalize_record():
        if not current_record_data: return

        wierzyciel = " ".join(current_record_data.get("wierzyciel_lines", [])).strip()
        produkt = " ".join(current_record_data.get("produkt_lines", [])).strip()
        date_line = current_record_data.get("data_line", [""])[0]
        
        date_match = RE_DATE_START.match(date_line)
        date_str = date_match.group(0) if date_match else ""
        
        if wierzyciel and produkt and date_str:
            found_in_catalog = any(known_prod in produkt for known_prod in KNOWN_PRODUCTS)
            amount_tokens = AMOUNT_RE.findall(date_line)
            amounts = (list(map(_parse_amount, amount_tokens)) + [None] * 4)[:4]

            final_records.append({
                "Źródło": source, "Rodaj_produktu": produkt, "Kredytodawca": wierzyciel,
                "Zawarcie_umowy": date_str, "Pierwotna_kwota": amounts[0], "Pozostało_do_spłaty": amounts[1],
                "Kwota_raty": amounts[2], "Suma_zaległości": amounts[3], "Produkt_zweryfikowany": found_in_catalog
            })

    for line in debt_lines:
        is_lender = bool(RE_LENDER.match(line)) and not RE_DATE_START.match(line)
        is_data_line = bool(RE_DATE_START.match(line))

        if state == "LOOKING_FOR_LENDER":
            if is_lender:
                current_record_data.setdefault("wierzyciel_lines", []).append(line)
            elif current_record_data:
                state = "LOOKING_FOR_PRODUCT"
                if not is_data_line:
                    current_record_data.setdefault("produkt_lines", []).append(line)
        
        elif state == "LOOKING_FOR_PRODUCT":
            if not is_data_line:
                current_record_data.setdefault("produkt_lines", []).append(line)

        if is_data_line:
            if current_record_data:
                current_record_data["data_line"] = [line]
                finalize_record()
            current_record_data = {}
            state = "LOOKING_FOR_LENDER"
            
    return final_records
