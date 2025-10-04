import fitz  # PyMuPDF
import re
import unicodedata
from typing import List, Dict, Any, Optional

# --- Zidentyfikowane stałe i reguły ---

# 1. Katalog produktów (kotwice) na podstawie dostarczonych raportów
KNOWN_PRODUCTS = frozenset([
    "Bezumowny debet w koncie", "Karta kredytowa", "Kredyt gotówkowy / pożyczka gotówkowa",
    "Kredyt gotówkowy, pożyczka bankowa", "Kredyt mieszkaniowy", "Kredyt na zakup towarów i usług",
    "Kredyt obrotowy", "Kredyt odnawialny", "Kredyt w rachunku bieżącym lub limit debetowy", "Pożyczka"
])

# 2. Wyrażenia regularne (Regex) dla kluczowych znaczników
RE_SECTION_START = re.compile(r"Zobowiązania finansowe - w trakcie spłaty", re.I)
RE_SECTION_END = re.compile(r"^(Łącznie|Zobowiązania finansowe - zamknięte|Informacje dodatkowe|Informacje szczegółowe)", re.I)
RE_DATE_START = re.compile(r"^\d{2}\.\d{2}\.\d{4}")
RE_LENDER = re.compile(r"^[A-ZĄĆĘŁŃÓŚŹŻ\s\d\.\-/]+$") # Wzorzec dla wierzyciela (wielkie litery)
AMOUNT_RE = re.compile(r"\b(ND|BRAK|(?:\d{1,3}(?:\s\d{3})*|\d+)(?:,\d{2})?)\b(?:\s*PLN)?", re.I)


def _deep_clean_text(text: str) -> str:
    """Normalizuje i czyści tekst z niestandardowych znaków."""
    text = unicodedata.normalize('NFKD', text)
    return re.sub(r'\s+', ' ', text).strip()

def _read_lines_from_pdf(pdf_bytes: bytes) -> List[str]:
    """Odczytuje PDF i zwraca listę czystych linii tekstu."""
    lines = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            # Użycie "blocks" jest bardziej niezawodne niż "text"
            blocks = page.get_text("blocks", sort=True)
            for b in blocks:
                block_text = b[4]
                for line in block_text.splitlines():
                    cleaned_line = _deep_clean_text(line)
                    if cleaned_line:
                        lines.append(cleaned_line)
    return lines

def _slice_active_debt_section(all_lines: List[str]) -> List[str]:
    """Wyodrębnia tylko sekcję z aktywnymi zobowiązaniami."""
    try:
        start_index = next(i for i, line in enumerate(all_lines) if RE_SECTION_START.search(line))
        
        end_index = len(all_lines)
        for i in range(start_index + 1, len(all_lines)):
            if RE_SECTION_END.search(all_lines[i]):
                end_index = i
                break
        
        # Ignoruj linię z nagłówkiem sekcji
        return all_lines[start_index + 1:end_index]
    except StopIteration:
        return [] # Sekcja nie została znaleziona

def _parse_amount(token: Optional[str]) -> Optional[float]:
    """Konwertuje tekst na kwotę, obsługując 'BRAK' i 'ND'."""
    if token is None:
        return None
    token_upper = token.upper()
    if "ND" in token_upper:
        return None
    if "BRAK" in token_upper:
        return 0.0
    
    # Usuwa PLN i białe znaki, zamienia przecinek na kropkę
    cleaned_token = token.replace("PLN", "").strip()
    cleaned_token = re.sub(r'\s', '', cleaned_token)
    cleaned_token = cleaned_token.replace(",", ".")
    
    try:
        return float(cleaned_token)
    except (ValueError, TypeError):
        return None

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    """
    Główna funkcja parsująca. Implementuje logikę maszyny stanów.
    """
    all_lines = _read_lines_from_pdf(pdf_bytes)
    debt_lines = _slice_active_debt_section(all_lines)

    if not debt_lines:
        return []

    final_records = []
    
    # --- Maszyna Stanów ---
    state = "LOOKING_FOR_LENDER"
    current_record_data = {}

    for line in debt_lines:
        if state == "LOOKING_FOR_LENDER":
            if RE_LENDER.match(line):
                current_record_data.setdefault("wierzyciel_lines", []).append(line)
            else: # Pierwsza linia, która nie jest wielkimi literami
                if "wierzyciel_lines" in current_record_data:
                    state = "LOOKING_FOR_PRODUCT"
                    # Ta linia już należy do produktu, więc przetwarzamy ją od razu
                    current_record_data.setdefault("produkt_lines", []).append(line)
                # Ignorujemy linie "śmieci" przed znalezieniem pierwszego wierzyciela

        elif state == "LOOKING_FOR_PRODUCT":
            if not RE_DATE_START.match(line):
                current_record_data.setdefault("produkt_lines", []).append(line)
            else: # Znaleziono linię z datą, kończymy zbieranie produktu
                state = "PROCESSING_DATA_LINE"
                # Ta linia zawiera dane, więc przetwarzamy ją od razu
                
                # Złącz zebrane linie w finalne ciągi znaków
                wierzyciel = " ".join(current_record_data.get("wierzyciel_lines", []))
                produkt = " ".join(current_record_data.get("produkt_lines", []))
                
                # Weryfikacja z katalogiem produktów
                found_in_catalog = any(known_prod in produkt for known_prod in KNOWN_PRODUCTS)

                # Ekstrakcja danych liczbowych
                date_str = RE_DATE_START.match(line).group(0)
                amount_tokens = AMOUNT_RE.findall(line)
                
                # Zapewnij, że zawsze są 4 wartości, dopełniając None
                amounts = (list(map(_parse_amount, amount_tokens)) + [None] * 4)[:4]

                final_records.append({
                    "Źródło": source,
                    "Rodzaj_produktu": produkt,
                    "Kredytodawca": wierzyciel,
                    "Zawarcie_umowy": date_str,
                    "Pierwotna_kwota": amounts[0],
                    "Pozostało_do_spłaty": amounts[1],
                    "Kwota_raty": amounts[2],
                    "Suma_zaległości": amounts[3],
                    "Produkt_zweryfikowany": found_in_catalog
                })

                # Reset i powrót do szukania kolejnego wierzyciela
                current_record_data = {}
                state = "LOOKING_FOR_LENDER"

    return final_records
