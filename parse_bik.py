# parse_bik.py (The Final, Block-Based, Correct TXT Parser)
import re
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
LENDER_KEYWORDS = ["ALIOR", "SANTANDER", "PKO", "COFIDIS", "SMARTNEY", "ALLEGRO", "BANK", "PAY", "SPÓŁKA", "S.A.", "SP.", "Z", "O.O."]

def _slice_active_section(lines: List[str]) -> List[str]:
    # Ta funkcja pozostaje kluczowa, aby odfiltrować niechciane sekcje
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

def parse_bik_txt(text_content: str, source: str = "auto") -> List[Dict[str, Any]]:
    # Dzielimy cały tekst na linie - to nasza podstawa
    all_lines = text_content.replace('\r', '').split('\n')
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    # Znajdujemy wszystkie linie z datami, które będą naszymi granicami rekordów
    anchor_indices = [i for i, line in enumerate(active_lines) if RE_DATE.search(line)]
    if not anchor_indices:
        return []

    final_rows = []
    # ITERUJEMY PO KAŻDYM REKORDZIE OSOBNO
    for i, anchor_index in enumerate(anchor_indices):
        start_bound = anchor_indices[i-1] + 1 if i > 0 else 0
        current_block = active_lines[start_bound : anchor_index + 1]
        if not current_block: continue
        
        all_text_lines, all_amount_tokens, date_str = [], [], ""
        
        # Zbieramy dane TYLKO z bieżącego bloku - to naprawia "urojone liczby"
        for line in current_block:
            all_amount_tokens.extend(_collect_amounts_from_line(line))
            
            date_match = RE_DATE.search(line)
            if date_match:
                date_str = date_match.group(1)

            # Zbieramy tekst, usuwając z niego daty i kwoty
            line_text_only = RE_DATE.sub('', line)
            line_text_only = AMOUNT_RE.sub('', line_text_only)
            line_text_only = re.sub(r'\bPLN\b', '', line_text_only, flags=re.I).strip()
            if line_text_only:
                all_text_lines.append(line_text_only)
        
        product, lender = "", ""
        full_text_chunk = " ".join(all_text_lines)
        words = [word for word in full_text_chunk.split() if word]
        
        # Poprawiona logika podziału produktu i kredytodawcy
        lender_start_index = -1
        for idx, word in enumerate(words):
            # Szukamy pierwszego słowa, które jest znanym słowem kluczowym kredytodawcy
            if any(keyword in word.upper() for keyword in LENDER_KEYWORDS):
                lender_start_index = idx
                break
        
        if lender_start_index != -1:
            product = " ".join(words[:lender_start_index])
            lender = " ".join(words[lender_start_index:])
        else: # Fallback, jeśli nie znajdziemy słowa kluczowego
            product = "Nie określono"
            lender = full_text_chunk

        parsed_amounts = [_parse_amount(tok) for tok in all_amount_tokens]
        final_amounts = (parsed_amounts + [None] * 4)[:4]

        final_rows.append({
            "Źródło": source, "Rodzaj_produktu": product, "Kredytodawca": lender, "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": final_amounts[0], "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2], "Suma_zaległości": final_amounts[3],
        })

    return final_rows
