# parse_bik.py (Final Intelligent Result Version)
import fitz
import re
import unicodedata
from typing import List, Dict, Any, Optional, TypedDict

class ParseResult(TypedDict):
    rows: List[Dict[str, Any]]
    lines_read: int
    active_lines_found: bool

RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe.*w\s+trakcie\s+spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe.*zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
AMOUNT_RE = re.compile(
    r"\b(ND|BRAK|(?:\d{1,3}(?:\s\d{3})*|\d+)(?:,\d{2})?)\b(?:\s*PLN)?",
    re.I
)

def _deep_clean_text(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    s = re.sub(r'[\u2010-\u2015]', '-', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'(?<=\d)\.(?=\d{3})', '', s)
    return s

def _read_lines(pdf_bytes: bytes) -> List[str]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines = []
    for page in doc:
        blocks = page.get_text("blocks", sort=True)
        for b in blocks:
            block_text = b[4]
            for line in block_text.splitlines():
                cleaned_line = _deep_clean_text(line)
                if cleaned_line:
                    lines.append(cleaned_line)
    return lines

def _slice_active_section(lines: List[str]) -> (List[str], bool):
    start_index = -1
    header_line_content = ""
    for i, line in enumerate(lines):
        if RE_ACTIVE.search(line):
            start_index = i
            header_line_content = line
            break
    if start_index == -1:
        return [], False

    header_match = RE_ACTIVE.search(header_line_content)
    first_line_of_data = header_line_content[header_match.end():].strip()
    end_index = len(lines)
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
    active_lines = [first_line_of_data] + lines[start_index + 1 : end_index]
    return [line for line in active_lines if line], True

def _parse_amount(tok: Optional[str]) -> Optional[float]:
    if tok is None: return None
    up = tok.upper().strip()
    if up == "ND": return None
    if up == "BRAK": return 0.0
    t = up.replace(" ", "").replace(",", ".")
    try: return float(t)
    except (ValueError, TypeError): return None

def _collect_amounts_from_line(line: str) -> List[Optional[str]]:
    return [match.group(1) for match in AMOUNT_RE.finditer(line)]

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> ParseResult:
    all_lines = _read_lines(pdf_bytes)
    lines_read_count = len(all_lines)
    
    active_lines, active_found = _slice_active_section(all_lines)
    if not active_found:
        return {"rows": [], "lines_read": lines_read_count, "active_lines_found": False}

    anchor_indices = [i for i, line in enumerate(active_lines) if RE_DATE.search(line)]
    if not anchor_indices:
        return {"rows": [], "lines_read": lines_read_count, "active_lines_found": True}

    final_rows = []
    for i, anchor_index in enumerate(anchor_indices):
        start_bound = anchor_indices[i-1] + 1 if i > 0 else 0
        current_block = active_lines[start_bound : anchor_index + 1]
        if not current_block: continue
        all_text_lines, all_amount_tokens, date_str = [], [], ""
        for line in current_block:
            amount_tokens_on_line = _collect_amounts_from_line(line)
            date_match = RE_DATE.search(line)
            line_without_amounts = AMOUNT_RE.sub("", line).strip()
            if line_without_amounts and not date_match:
                 all_text_lines.append(line_without_amounts.strip())
            if amount_tokens_on_line:
                all_amount_tokens.extend(amount_tokens_on_line)
            if date_match:
                date_str = date_match.group(1)
        product, lender = "", ""
        if all_text_lines:
            all_text_lines = [line for line in all_text_lines if line]
            if all_text_lines:
                product = all_text_lines.pop(-1)
                lender = " ".join(all_text_lines)
        parsed_amounts = [_parse_amount(tok) for tok in all_amount_tokens]
        final_amounts = (parsed_amounts + [None] * 4)[:4]
        final_rows.append({
            "Źródło": source, "Rodzaj_produktu": product, "Kredytodawca": lender, "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": final_amounts[0], "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2], "Suma_zaległości": final_amounts[3],
        })

    return {"rows": final_rows, "lines_read": lines_read_count, "active_lines_found": True}
