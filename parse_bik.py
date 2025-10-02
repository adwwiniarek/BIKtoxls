# parse_bik.py (final version with reverse analysis logic)
import fitz  # PyMuPDF
import re
from typing import List, Dict, Any, Optional

# --- Stałe i wyrażenia regularne ---
NBSP_CHARS = "\u00A0\u202F\u2009"
RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
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
    if start_index is None: return []
    end_index = len(lines)
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
    return lines[start_index + 1 : end_index]

# --- Funkcje pomocnicze do identyfikacji i parsowania danych ---

def _is_just_amounts(line: str) -> bool:
    line_without_amounts = AMOUNT_RE.sub("", line).strip()
    line_without_currency = re.sub(r'(PLN)', '', line_without_amounts, flags=re.I).strip()
    return not bool(line_without_currency)

def _parse_amount(tok: Optional[str], position: int) -> Optional[float]:
    if tok is None: return None
    up = tok.upper().strip()
    if up == "ND": return None
    if up == "BRAK": return 0.0
    t = up.replace("PLN", "").strip().replace(",", ".")
    try:
        return float(t)
    except (ValueError, TypeError):
        return None

def _collect_amounts_from_line(line: str) -> List[Optional[float]]:
    tokens = [match.group(1) for match in AMOUNT_RE.finditer(line)]
    padded_tokens = tokens + [None] * (4 - len(tokens))
    return [_parse_amount(tok, i) for i, tok in enumerate(padded_tokens[:4])]

# --- GŁÓWNA LOGIKA PARSOWANIA ---

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    all_lines = _read_lines(pdf_bytes)
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    # Krok 1: Znajdź wszystkie "kotwice" (indeksy linii z datami)
    anchor_indices = [i for i, line in enumerate(active_lines) if RE_DATE.search(line)]
    if not anchor_indices:
        return []

    final_rows = []
    # Krok 2: Dla każdej kotwicy, stwórz i przeanalizuj jej blok kontekstowy
    for i, anchor_index in enumerate(anchor_indices):
        # Określ granice bloku: od poprzedniej kotwicy (lub początku) do bieżącej kotwicy
        start_bound = anchor_indices[i-1] + 1 if i > 0 else 0
        current_block = active_lines[start_bound : anchor_index + 1]

        if not current_block:
            continue

        # Inicjalizacja danych dla bieżącego rekordu
        date_str = ""
        product = ""
        lender_parts = []
        amounts = [None] * 4

        # Krok 3: Analiza wsteczna wewnątrz bloku
        # Ostatnia linia bloku to nasza kotwica - linia z datą
        date_line = current_block[-1]
        date_match = RE_DATE.search(date_line)
        if date_match:
            date_str = date_match.group(1)
        
        # Zbierz kwoty z całego bloku, priorytetyzując od dołu do góry
        for line in reversed(current_block):
            line_amounts = _collect_amounts_from_line(line)
            for j in range(4):
                if amounts[j] is None and line_amounts[j] is not None:
                    amounts[j] = line_amounts[j]
        
        # Linie kontekstowe (wszystko oprócz linii z datą)
        context_lines = current_block[:-1]
        
        # Szukaj produktu i kredytodawcy od dołu do góry w liniach kontekstowych
        found_product = False
        for line in reversed(context_lines):
            # Jeśli linia zawiera tylko kwoty, ignorujemy ją
            if _is_just_amounts(line):
                continue
            
            # Pierwsza linia "tekstowa" od dołu to produkt
            if not found_product:
                product = line
                found_product = True
            # Wszystkie kolejne linie "tekstowe" to części nazwy kredytodawcy
            else:
                lender_parts.insert(0, line)

        final_rows.append({
            "Źródło": source,
            "Rodzaj_produktu": product.strip(),
            "Kredytodawca": " ".join(lender_parts).strip(),
            "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": amounts[0],
            "Pozostało_do_spłaty": amounts[1],
            "Kwota_raty": amounts[2],
            "Suma_zaległości": amounts[3],
        })

    return final_rows
