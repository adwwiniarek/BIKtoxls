# parse_bik.py (The Final Version - Handles Both TXT Structures)
import re
from typing import List, Dict, Any, Optional

RE_ACTIVE = re.compile(r"Zobowiązania\s*finansowe.*?w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s*finansowe.*?zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s*dodatkowe|Informacje\s*szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
AMOUNT_RE = re.compile(r"(\b(?:\d[\d\s.,]*)(?:PLN|, ?\d{2})\b|ND|BRAK)", re.I)
LENDER_KEYWORDS = ["ALIOR", "SANTANDER", "PKO", "COFIDIS", "SMARTNEY", "ALLEGRO", "BANK", "PAY", "SPÓŁKA", "S.A.", "SP.", "Z", "O.O."]
GARBAGE_HEADERS_RE = re.compile(r"Typ\s*Zawarcie\s*umowy.*?Ostatnia\s*płatność", re.I)

def _slice_active_section(lines: List[str]) -> List[str]:
    full_text = "\n".join(lines)
    active_section_match = RE_ACTIVE.search(full_text)
    if not active_section_match:
        return []

    start_pos = active_section_match.end()
    end_pos = len(full_text)
    closed_section_match = RE_CLOSED.search(full_text, pos=start_pos)
    if closed_section_match:
        end_pos = closed_section_match.start()
        
    active_text = full_text[start_pos:end_pos]
    active_text = GARBAGE_HEADERS_RE.sub("", active_text)
    return active_text.split('\n')

def _parse_amount(tok: Optional[str]) -> Optional[float]:
    if tok is None: return None
    up = tok.upper().strip()
    if up == "ND": return None
    if up == "BRAK": return 0.0
    t = re.sub(r'[^\d,]', '', up).replace(",", ".")
    try:
        return float(t)
    except (ValueError, TypeError):
        return None

def parse_bik_txt(text_content: str, source: str = "auto") -> List[Dict[str, Any]]:
    all_lines = text_content.replace('\r', '').split('\n')
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

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
            all_amount_tokens.extend(AMOUNT_RE.findall(line))
            date_match = RE_DATE.search(line)
            if date_match:
                date_str = date_match.group(1)

            line_text_only = RE_DATE.sub('', line)
            line_text_only = AMOUNT_RE.sub('', line_text_only)
            line_text_only = re.sub(r'\bPLN\b', '', line_text_only, flags=re.I).strip()
            if line_text_only:
                all_text_lines.append(line_text_only)
        
        product, lender = "", ""
        full_text_chunk = " ".join(all_text_lines)
        words = [word for word in full_text_chunk.split() if word]
        
        # --- NOWA, INTELIGENTNA LOGIKA PODZIAŁU ---
        lender_start_index = -1
        lender_end_index = -1
        
        for idx, word in enumerate(words):
            if any(keyword in word.upper() for keyword in LENDER_KEYWORDS):
                if lender_start_index == -1:
                    lender_start_index = idx
                lender_end_index = idx

        if lender_start_index != -1:
            # Heurystyka: jeśli słowo kluczowe jest na początku, to kolejność jest [Kredytodawca] [Produkt]
            if lender_start_index <= 1:
                lender = " ".join(words[lender_start_index : lender_end_index + 1])
                product = " ".join(words[lender_end_index + 1 :])
            # W przeciwnym wypadku, kolejność to [Produkt] [Kredytodawca]
            else:
                product = " ".join(words[:lender_start_index])
                lender = " ".join(words[lender_start_index:])
        else: # Fallback
            product = full_text_chunk
            lender = "Nie znaleziono"

        parsed_amounts = [_parse_amount(tok) for tok in all_amount_tokens]
        final_amounts = (parsed_amounts + [None] * 4)[:4]

        final_rows.append({
            "Źródło": source, "Rodzaj_produktu": product.strip(), "Kredytodawca": lender.strip(), "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": final_amounts[0], "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2], "Suma_zaległości": final_amounts[3],
        })

    return final_rows
    
