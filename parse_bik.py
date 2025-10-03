# parse_bik.py (The Absolute Final - Header Removal Fix)
import fitz
import re
import unicodedata
from typing import List, Dict, Any, Optional

RE_ACTIVE = re.compile(r"Zobowiązania\s*finansowe.*?w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s*finansowe.*?zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s*dodatkowe|Informacje\s*szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
# Bardziej rygorystyczny wzorzec dla kwot
AMOUNT_RE = re.compile(r"(\b(?:\d[\d\s.,]*)(?:PLN|, ?\d{2})\b|ND|BRAK)", re.I)
LENDER_KEYWORDS = ["ALIOR", "SANTANDER", "PKO", "COFIDIS", "SMARTNEY", "ALLEGRO", "BANK", "PAY", "SPÓŁKA", "S.A.", "SP.", "Z", "O.O."]
# Wzorzec do usuwania "śmieciowych" nagłówków tabeli
GARBAGE_HEADERS_RE = re.compile(r"Typ\s*Zawarcie\s*umowy.*?Ostatnia\s*płatność", re.I)

def _deep_clean_text(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    s = re.sub(r'[\u2010-\u2015]', '-', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def _read_and_flatten_text(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    full_text = []
    for page in doc:
        # Używamy zwykłego "text" - okazuje się bardziej przewidywalny dla struktury BIK
        full_text.append(page.get_text("text", sort=True))
    return _deep_clean_text(" ".join(full_text))

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

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    full_text = _read_and_flatten_text(pdf_bytes)
    
    try:
        active_section_match = re.search(r"Zobowiązania\s*finansowe.*?w\s*trakcie\s*spłaty", full_text, re.I)
        closed_section_match = re.search(r"Zobowiązania\s*finansowe.*?zamknięte", full_text, re.I)
        
        if not active_section_match:
            return []
            
        start_pos = active_section_match.end()
        end_pos = closed_section_match.start() if closed_section_match else len(full_text)
        
        active_text = full_text[start_pos:end_pos]
        
        # --- TWOJA SUGESTIA WPROWADZONA W ŻYCIE ---
        # Usuwamy "śmieciowe" nagłówki z analizowanego tekstu
        active_text = GARBAGE_HEADERS_RE.sub("", active_text)
        # -----------------------------------------

    except Exception:
        return []

    date_matches = list(RE_DATE.finditer(active_text))
    if not date_matches:
        return []

    final_rows = []
    for i, current_match in enumerate(date_matches):
        date_str = current_match.group(1)
        
        start_chunk_pos = date_matches[i-1].end() if i > 0 else 0
        end_chunk_pos = current_match.start()
        
        text_chunk_before_date = active_text[start_chunk_pos:end_chunk_pos]
        
        amounts_chunk_end_pos = date_matches[i+1].start() if i + 1 < len(date_matches) else len(active_text)
        amounts_chunk_for_this_record = active_text[current_match.start():amounts_chunk_end_pos]
        
        amount_tokens = AMOUNT_RE.findall(amounts_chunk_for_this_record)
        parsed_amounts = [_parse_amount(tok) for tok in amount_tokens]
        # Bierzemy tylko pierwsze 4 znalezione kwoty po dacie
        final_amounts = (parsed_amounts[0:4] + [None] * 4)[:4]

        words = [word for word in text_chunk_before_date.split() if word]
        product, lender = "", ""
        lender_start_index = -1
        for idx, word in enumerate(words):
            if any(keyword in word.upper() for keyword in LENDER_KEYWORDS):
                lender_start_index = idx
                break
        
        if lender_start_index != -1:
            product = " ".join(words[:lender_start_index])
            lender = " ".join(words[lender_start_index:])
        elif words:
            product = " ".join(words[:-1]) if len(words) > 1 else "Nie określono"
            lender = words[-1]

        final_rows.append({
            "Źródło": source, "Rodzaj_produktu": product.strip(), "Kredytodawca": lender.strip(), "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": final_amounts[0], "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2], "Suma_zaległości": final_amounts[3],
        })
        
    return final_rows
