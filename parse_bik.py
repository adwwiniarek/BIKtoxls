# parse_bik.py (final robust version - v2)
import fitz  # PyMuPDF
import re
from typing import List, Dict, Any, Optional

# --- Stałe i wyrażenia regularne ---

NBSP_CHARS = "\u00A0\u202F\u2009"
RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_FORBIDDEN_IN_UPPER = re.compile(r"(PLN|ND|BRAK|\d)")
AMOUNT_RE = re.compile(
    r"(ND|BRAK|(?:\d{1,3}(?:[ .]\d{3})*|\d+)(?:[.,]\d{2})?)(?:\s*PLN)?",
    re.I
)

# --- Funkcje pomocnicze do normalizacji i czytania tekstu ---

def _normalize_text(s: str) -> str:
    if any(c in s for c in NBSP_CHARS):
        s = re.sub(f"[{NBSP_CHARS}]", " ", s)
    s = re.sub(r"(?<=\d)[ .](?=\d{3}\b)", "", s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

def _read_lines(pdf_bytes: bytes) -> List[str]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines = []
    for page in doc:
        raw_lines = page.get_text("text").splitlines()
        for line in raw_lines:
            normalized_line = _normalize_text(line)
            if normalized_line:
                lines.append(normalized_line)
    return lines

def _slice_active_section(lines: List[str]) -> List[str]:
    start_index = next((i for i, line in enumerate(lines) if RE_ACTIVE.search(line)), None)
    if start_index is None:
        return []

    end_index = len(lines)
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_ACTIVE.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
    return lines[start_index + 1 : end_index]

# --- Funkcje pomocnicze do identyfikacji typów linii ---

def _is_lender(line: str) -> bool:
    if RE_FORBIDDEN_IN_UPPER.search(line):
        return False
    letters = re.sub(r"[^a-zA-ZĄąĆćĘęŁłŃńÓóŚśŹźŻż]", "", line)
    return bool(letters) and line == line.upper()

def _is_just_amounts(line: str) -> bool:
    """Sprawdza, czy linia składa się wyłącznie z kwot i waluty."""
    # Jeśli data jest w linii, to nie jest to linia "tylko z kwotami"
    if RE_DATE.search(line):
        return False
    
    # Usuń wszystko, co pasuje do wzorca kwoty
    line_without_amounts = AMOUNT_RE.sub("", line).strip()
    # Usuń pozostałości, takie jak "PLN", które mogły nie zostać usunięte przez regex
    line_without_currency = re.sub(r'(PLN)', '', line_without_amounts, flags=re.I).strip()
    
    # Jeśli po usunięciu kwot i walut nic nie zostało, to była to linia tylko z kwotami.
    return not bool(line_without_currency)

# --- Funkcje pomocnicze do parsowania danych finansowych ---

def _parse_amount(tok: Optional[str], position: int) -> Optional[float]:
    if tok is None:
        return None
    up = tok.upper().strip()
    if up == "ND":
        return None
    if up == "BRAK":
        return 0.0 if position == 3 else None
    
    t = up.replace("PLN", "").strip().replace(",", ".")
    try:
        return float(t)
    except (ValueError, TypeError):
        return None

def _collect_amounts_from_line(line: str) -> List[Optional[float]]:
    tokens = [match.group(1) for match in AMOUNT_RE.finditer(line)]
    padded_tokens = tokens + [None] * (4 - len(tokens))
    return [_parse_amount(tok, i) for i, tok in enumerate(padded_tokens[:4])]

# --- Główna funkcja parsowania ---

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    all_lines = _read_lines(pdf_bytes)
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    blocks = []
    current_block = []
    if active_lines:
        for line in active_lines:
            if _is_lender(line) and current_block:
                blocks.append(current_block)
                current_block = [line]
            else:
                current_block.append(line)
        if current_block:
            blocks.append(current_block)

    final_rows = []
    for block in blocks:
        lender = ""
        product = ""
        date_str = ""
        
        # Inicjalizuj puste kwoty
        amounts = [None] * 4

        # Linie, które nie są kredytodawcą
        other_lines = []

        lender_lines = []
        is_lender_part = True
        for line in block:
            if _is_lender(line) and is_lender_part:
                lender_lines.append(line)
            else:
                is_lender_part = False
                other_lines.append(line)
        lender = " ".join(lender_lines)

        # Znajdź produkt (linia, która nie jest kwotą ani datą)
        for line in other_lines:
            if not RE_DATE.search(line) and not _is_just_amounts(line):
                product = line
                break
        
        # Znajdź datę
        for line in other_lines:
            if RE_DATE.search(line):
                date_str = RE_DATE.search(line).group(0).strip()
                break # Zakładamy jedną datę na blok
        
        if not date_str:
             continue # Jeśli w bloku nie ma daty, pomijamy go
             
        # Zbierz kwoty z wszystkich linii w bloku, które nie są kredytodawcą ani produktem
        # To pozwoli połączyć kwoty z różnych linii
        for line in other_lines:
             if line != product:
                line_amounts = _collect_amounts_from_line(line)
                for i in range(4):
                    if amounts[i] is None and line_amounts[i] is not None:
                        amounts[i] = line_amounts[i]

        final_rows.append({
            "Źródło": source,
            "Rodzaj_produktu": product.strip(),
            "Kredytodawca": lender.strip(),
            "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": amounts[0],
            "Pozostało_do_spłaty": amounts[1],
            "Kwota_raty": amounts[2],
            "Suma_zaległości": amounts[3],
        })

    return final_rows
