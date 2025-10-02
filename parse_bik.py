# parse_bik.py (final robust version)
import fitz  # PyMuPDF
import re
from typing import List, Dict, Any, Optional

# --- Stałe i wyrażenia regularne ---

# Znaki spacji niełamliwych do normalizacji
NBSP_CHARS = "\u00A0\u202F\u2009"  # NBSP, narrow no-break, thin space

# Wyrażenia do identyfikacji sekcji w raporcie
RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL = re.compile(r"^Łącznie\b", re.I)

# Wyrażenia do identyfikacji danych
RE_DATE = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_ANY_DIGIT = re.compile(r"\d")
RE_FORBIDDEN_IN_UPPER = re.compile(r"(PLN|ND|BRAK|\d)")

# Wyrażenie do parsowania kwot (ND, BRAK, lub liczba z opcjonalnym "PLN")
AMOUNT_RE = re.compile(
    r"(ND|BRAK|(?:\d{1,3}(?:[ .]\d{3})*|\d+)(?:[.,]\d{2})?)(?:\s*PLN)?",
    re.I
)

# --- Funkcje pomocnicze do normalizacji i czytania tekstu ---

def _normalize_text(s: str) -> str:
    """Normalizuje tekst: zamienia różne rodzaje spacji, usuwa separatory tysięcy, standaryzuje wielokrotne spacje."""
    if any(c in s for c in NBSP_CHARS):
        s = re.sub(f"[{NBSP_CHARS}]", " ", s)
    s = re.sub(r"(?<=\d)[ .](?=\d{3}\b)", "", s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

def _read_lines(pdf_bytes: bytes) -> List[str]:
    """Odczytuje wszystkie linie tekstu z pliku PDF i normalizuje je."""
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
    """Wyodrębnia sekcję z aktywnymi zobowiązaniami."""
    start_index = next((i for i, line in enumerate(lines) if RE_ACTIVE.search(line)), None)
    if start_index is None:
        return []

    end_index = len(lines)
    # Znajdź koniec sekcji, który jest początkiem następnej znanej sekcji
    for j in range(start_index + 1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_ACTIVE.search(lines[j]) or RE_TOTAL.search(lines[j]):
            end_index = j
            break
            
    return lines[start_index + 1 : end_index]

# --- Funkcje pomocnicze do identyfikacji typów linii ---

def _is_lender(line: str) -> bool:
    """Sprawdza, czy linia jest prawdopodobną nazwą kredytodawcy (pisana wielkimi literami)."""
    if RE_FORBIDDEN_IN_UPPER.search(line):
        return False
    # Sprawdź, czy w linii są jakiekolwiek litery
    letters = re.sub(r"[^a-zA-ZĄąĆćĘęŁłŃńÓóŚśŹźŻż]", "", line)
    return bool(letters) and line == line.upper()

# --- Funkcje pomocnicze do parsowania danych finansowych ---

def _parse_amount(tok: Optional[str], position: int) -> Optional[float]:
    """Parsuje pojedynczy token kwoty na liczbę."""
    if tok is None:
        return None
    up = tok.upper().strip()
    if up == "ND":
        return None
    if up == "BRAK":
        return 0.0 if position == 3 else None  # Tylko dla "Suma zaległości" BRAK oznacza 0.0
    
    # Usuń "PLN", zamień przecinek na kropkę
    t = up.replace("PLN", "").strip().replace(",", ".")
    try:
        return float(t)
    except (ValueError, TypeError):
        return None

def _collect_amounts_from_line(line: str) -> List[Optional[float]]:
    """Zbiera wszystkie kwoty z linii danych."""
    # Znajdź wszystkie dopasowania do wzorca kwoty
    tokens = [match.group(1) for match in AMOUNT_RE.finditer(line)]
    
    # Uzupełnij do 4 pozycji, jeśli jest ich mniej
    padded_tokens = tokens[:4] + [None] * (4 - len(tokens))
    
    return [_parse_amount(tok, i) for i, tok in enumerate(padded_tokens)]

# --- Główna funkcja parsowania ---

def parse_bik_pdf(pdf_bytes: bytes, source: str = "auto") -> List[Dict[str, Any]]:
    """
    Parsuje raport BIK w formacie PDF i zwraca listę ustrukturyzowanych zobowiązań.

    Args:
        pdf_bytes: Surowe bajty pliku PDF.
        source: Identyfikator źródła (np. "prywatny", "firmowy"), który zostanie dodany do każdego rekordu.

    Returns:
        Lista słowników, gdzie każdy słownik reprezentuje jedno zobowiązanie.
    """
    all_lines = _read_lines(pdf_bytes)
    active_lines = _slice_active_section(all_lines)
    
    if not active_lines:
        return []

    # Krok 1: Podziel wszystkie linie na bloki logiczne (każdy blok to jedno zobowiązanie)
    blocks = []
    current_block = []
    for line in active_lines:
        # Nowy blok zaczyna się, gdy znajdujemy nowego kredytodawcę, a obecny blok nie jest pusty.
        if _is_lender(line) and current_block:
            blocks.append(current_block)
            current_block = [line]
        else:
            current_block.append(line)
    
    if current_block:
        blocks.append(current_block)

    # Krok 2: Przetwórz każdy blok, aby wyodrębnić dane
    final_rows = []
    for block in blocks:
        lender = ""
        product = ""
        
        # Znajdź kredytodawcę (może być wieloliniowy)
        lender_lines = []
        for i, line in enumerate(block):
            if _is_lender(line):
                lender_lines.append(line)
            else:
                break # Koniec nazwy kredytodawcy
        lender = " ".join(lender_lines)
        
        # Znajdź produkt (pierwsza linia po kredytodawcy, która nie jest datą)
        # Zaczynamy szukać od końca nazwy kredytodawcy
        non_lender_lines = block[len(lender_lines):]
        for line in non_lender_lines:
            if not RE_DATE.search(line):
                product = line
                break
        
        # Znajdź wszystkie wiersze z danymi finansowymi
        for line in block:
            if RE_DATE.search(line):
                date_match = RE_DATE.search(line)
                date_str = date_match.group(0).strip()
                amounts = _collect_amounts_from_line(line)
                
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
