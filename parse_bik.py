# parse_bik.py (The Definitive Edition)
import fitz  # PyMuPDF
import re
import unicodedata
from typing import List, Dict, Any, Optional

# --- Stałe i wyrażenia regularne ---
RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)
RE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
AMOUNT_RE = re.compile(
    r"\b(ND|BRAK|(?:\d{1,3}(?:\s\d{3})*|\d+)(?:,\d{2})?)\b(?:\s*PLN)?",
    re.I
)

# --- ETAP 1: Agresywne oczyszczanie i ekstrakcja danych ---

def _deep_clean_text(s: str) -> str:
    """
    Kluczowa funkcja do "głębokiego czyszczenia" tekstu wyciągniętego z PDF.
    Rozwiązuje problemy z "niewidzialnymi" znakami i różnymi typami spacji.
    """
    # Normalizacja Unicode do najbardziej kompatybilnej formy (np. usuwa ligatury)
    s = unicodedata.normalize('NFKD', s)
    # Zamienia wszystkie rodzaje białych znaków (spacje, tabulatory, etc.) na pojedynczą spację
    s = re.sub(r'\s+', ' ', s).strip()
    # Usuwa kropki jako separatory tysięcy (np. "1.000" -> "1000")
    s = re.sub(r'(?<=\d)\.(?=\d{3})', '', s)
    return s

def _read_lines(pdf_bytes: bytes) -> List[str]:
    """Odczytuje i natychmiastowo czyści wszystkie linie z PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines = []
    for page in doc:
        raw_lines = page.get_text("text").splitlines()
        for line in raw_lines:
            cleaned_line = _deep_clean_text(line)
            if cleaned_line:
                lines.append(cleaned_line)
    return lines

def _slice_active_section(lines: List[str]) -> List[str]:
    """Wyodrębnia sekcję z aktywnymi zobowiązaniami."""
    start_index = next((i for i, line in enumerate(lines) if RE_ACTIVE.search(line)), None)
    if start_index is None: return []
    end_index = len(lines)
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
    return lines[start_index + 1 : end_index]

def _parse_amount(tok: Optional[str]) -> Optional[float]:
    """Parsuje pojedynczy token kwoty na liczbę."""
    if tok is None: return None
    up = tok.upper().strip()
    if up == "ND": return None
    if up == "BRAK": return 0.0
    # Usuwa spacje (teraz już na pewno separatory tysięcy) i zamienia przecinek na kropkę
    t = up.replace(" ", "").replace(",", ".")
    try:
        return float(t)
    except (ValueError, TypeError):
        return None

def _collect_amounts_from_line(line: str) -> List[Optional[str]]:
    """Zbiera surowe tokeny kwot (jako stringi) z linii."""
    return [match.group(1) for match in AMOUNT_RE.finditer(line)]

# --- ETAP 2: Inteligentna interpretacja w kontekście ---

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    # Krok 1.1: Wczytaj i oczyść dane u źródła
    all_lines = _read_lines(pdf_bytes)
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    # Krok 1.2: Znajdź wszystkie "kotwice" (indeksy linii z datami)
    anchor_indices = [i for i, line in enumerate(active_lines) if RE_DATE.search(line)]
    if not anchor_indices:
        return []

    final_rows = []
    # Krok 2.1: Dla każdej kotwicy, stwórz i przeanalizuj jej blok kontekstowy
    for i, anchor_index in enumerate(anchor_indices):
        start_bound = anchor_indices[i-1] + 1 if i > 0 else 0
        current_block = active_lines[start_bound : anchor_index + 1]

        if not current_block:
            continue

        # Krok 2.2: Zbierz wszystkie "surowe" informacje z bloku
        all_text_lines = []
        all_amount_tokens = []
        date_str = ""

        for line in current_block:
            amount_tokens_on_line = _collect_amounts_from_line(line)
            date_match = RE_DATE.search(line)

            # Czysty tekst to taki, który nie jest tylko kwotami
            line_without_amounts = AMOUNT_RE.sub("", line).strip()
            if line_without_amounts and not date_match:
                 all_text_lines.append(line_without_amounts.strip())

            if amount_tokens_on_line:
                all_amount_tokens.extend(amount_tokens_on_line)
            
            if date_match:
                date_str = date_match.group(1)

        # Krok 2.3: Zinterpretuj zebrane informacje
        product = ""
        lender = ""
        
        # Ostatnia linia tekstu to produkt, reszta to kredytodawca
        if all_text_lines:
            product = all_text_lines.pop(-1)
            lender = " ".join(all_text_lines)

        # Sparsuj zebrane tokeny kwot
        parsed_amounts = [_parse_amount(tok) for tok in all_amount_tokens]
        # Wypełnij brakujące miejsca do 4, jeśli jest ich mniej
        final_amounts = (parsed_amounts + [None] * 4)[:4]

        # Montaż finalnego rekordu
        final_rows.append({
            "Źródło": source,
            "Rodzaj_produktu": product,
            "Kredytodawca": lender,
            "Zawarcie_umowy": date_str,
            "Pierwotna_kwota": final_amounts[0],
            "Pozostało_do_spłaty": final_amounts[1],
            "Kwota_raty": final_amounts[2],
            "Suma_zaległości": final_amounts[3],
        })

    return final_rows
