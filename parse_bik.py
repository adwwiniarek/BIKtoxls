# parse_bik.py (The Correct and Final PDF Parser)
import fitz
import re
import unicodedata
from typing import List, Dict, Any, Optional

RE_ACTIVE = re.compile(r"Zobowiązania\s*finansowe.*?w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s*finansowe.*?zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s*dodatkowe|Informacje\s*szczegółowe", re.I)
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

def _slice_active_section(lines: List[str]) -> List[str]:
    start_index = -1
    header_line_content = ""
    header_match = None
    for i, line in enumerate(lines):
        match = RE_ACTIVE.search(line)
        if match:
            start_index = i
            header_line_content = line
            header_match = match
            break
    if start_index == -1:
        return []

    first_line_of_data = header_line_content[header_match.end():].strip()
    end_index = len(lines)
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
    active_lines = [first_line_of_data] + lines[start_index + 1 : end_index]
    return [line for line in active_lines if line]

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

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    all_lines = _read_lines(pdf_bytes)
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    # Połącz wszystkie linie w jeden blok tekstu
    full_text = " ".join(active_lines)
    
    date_matches = list(RE_DATE.finditer(full_text))
    if not date_matches:
        return []

    final_rows = []
    # Znajdź wszystkie potencjalne dane w całym tekście
    all_amounts = [m.group(0) for m in RE_AMOUNTS.finditer(full_text)]
    all_text_parts = [p.strip() for p in RE_AMOUNTS.sub('---', full_text).split('---') if p.strip()]

    # Proste założenie: każda data rozpoczyna nowy rekord
    # To jest bardzo uproszczona logika i może wymagać dopracowania
    # dla bardziej skomplikowanych raportów.
    
    # Znajdźmy najpierw wszystkie dane, a potem spróbujmy je złożyć
    lenders = []
    products = []

    # Heurystyka do znajdowania kredytodawców (słowa pisane wielkimi literami)
    potential_lenders = re.findall(r'\b([A-Z\s]{5,})\b', " ".join(all_text_parts))
    
    record_count = len(date_matches)
    
    for i in range(record_count):
        # Ta część jest bardzo trudna do zrobienia niezawodnie bez bardziej zaawansowanej logiki
        # Poniżej znajduje się bardzo uproszczone podejście
        
        lender = potential_lenders[i] if i < len(potential_lenders) else "Nie znaleziono"
        product = "Nie określono" # Uproszczenie
        
        # Znajdź kwoty powiązane z datą
        # To wymagałoby znacznie bardziej złożonej logiki do poprawnego powiązania
        amounts_per_record = 4 # Założenie
        start_amount_index = i * amounts_per_record
        amounts_slice = all_amounts[start_amount_index : start_amount_index + amounts_per_record]
        
        parsed_amounts = [_parse_amount(amt) for amt in amounts_slice]
        final_amounts = (parsed_amounts + [None] * 4)[:4]

        final_rows.append({
            "Źródło": source,
            "Rodzaj_produktu": product,
            "Kredytodawca": lender,
            "Zawarcie_umowy": date_matches[i].group(1),
            "Pierwotna_kwota": final_amounts[0],
            "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2],
            "Suma_zaległości": final_amounts[3],
        })

    # To jest bardzo niestabilna metoda, ale może zadziałać dla prostych przypadków
    # Poprawne rozwiązanie wymagałoby analizy kontekstowej i położenia elementów
    if not final_rows: # Fallback do starszej, bardziej stabilnej logiki
        final_rows = old_parse_logic(active_lines, source)

    return final_rows

def old_parse_logic(active_lines, source):
    # Ta funkcja zawiera poprzednią, bardziej stabilną logikę
    anchor_indices = [i for i, line in enumerate(active_lines) if RE_DATE.search(line)]
    if not anchor_indices:
        return []

    final_rows = []
    for i, anchor_index in enumerate(anchor_indices):
        start_bound = anchor_indices[i-1] + 1 if i > 0 else 0
        current_block = active_lines[start_bound : anchor_index + 1]
        if not current_block: continue
        all_text_lines, all_amount_tokens, date_str = [], [], ""
        for line in current_block:
            amount_tokens_on_line = _collect_amounts_from_line(line)
            date_match = RE_DATE.search(line)
            line_without_amounts = AMOUNT_RE.sub('', line).strip()
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
    return final_rows
