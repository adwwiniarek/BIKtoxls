# parse_bik.py (fixed and improved)
import fitz, re

# --- Normalizacja tekstu (spacje niełamliwe, wąskie spacje, separatory tysięcy) ---

NBSP_CHARS = "\u00A0\u202F\u2009"  # NBSP, narrow no-break, thin space

def _normalize_text(s: str) -> str:
    # 1) Zamień różne spacje na zwykłą
    if any(c in s for c in NBSP_CHARS):
        s = re.sub(f"[{NBSP_CHARS}]", " ", s)
    # 2) Usuń separatory tysięcy (kropka/spacja) TYLKO między cyframi (np. 25.298 -> 25298, 2 045 -> 2045)
    s = re.sub(r"(?<=\d)[ .](?=\d{3}\b)", "", s)
    # 3) Standaryzuj wielokrotne spacje
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

# --- Sekcje / kotwice ---
RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO   = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL  = re.compile(r"^Łącznie\b", re.I)
RE_DATE   = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_ANYD   = re.compile(r"\d{2}\.\d{2}\.\d{4}")
RE_FORBID = re.compile(r"(PLN|ND|BRAK|\d)")

# Kwota: ND | BRAK | liczba (z opcjonalnymi tysiącami i groszami) + opcjonalne "PLN"
AMOUNT_RE = re.compile(
    r"(ND|BRAK|(?:\d{1,3}(?:[ .]\d{3})+|\d+)(?:[.,]\d+)?)(?:\s*PLN)?",
    re.I
)

def _read_lines(pdf_bytes: bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines = []
    for p in doc:
        raw = p.get_text("text").splitlines()
        for l in raw:
            l = l.strip()
            if not l:
                continue
            lines.append(_normalize_text(l))
    return lines

def _slice_active(lines):
    s = next((i for i,l in enumerate(lines) if RE_ACTIVE.search(l)), None)
    if s is None: return []
    e = len(lines)
    for j in range(s+1, len(lines)):
        if RE_CLOSED.search(lines[j]) or RE_INFO.search(lines[j]) or RE_ACTIVE.search(lines[j]):
            e = j; break
    for k in range(s+1, e):
        if RE_TOTAL.search(lines[k]): e = k; break
    return lines[s:e]

def _is_upper(line: str) -> bool:
    if RE_FORBID.search(line) or RE_ANYD.search(line):
        return False
    letters = re.sub(r"[^A-Za-zĄąĆćĘęŁłŃńÓóŚśŹźŻż]", "", line)
    return bool(letters) and line == line.upper()

def _parse_amount(tok: str, pos: int):
    up = tok.upper().strip()
    if up == "ND":   return None
    if up == "BRAK": return 0.0 if pos == 3 else None
    t = up.replace("PLN", "").strip()
    t = t.replace(",", ".")
    try:
        return float(t)
    except:
        return None

def _collect_from_same_line(line: str):
    m = RE_DATE.match(line)
    rest = line[m.end():].strip() if m else line

    toks = [m.group(1) for m in AMOUNT_RE.finditer(rest)]
    toks = toks[:4] + [None]*(4-len(toks))
    return [_parse_amount(t, i) if t else None for i,t in enumerate(toks)]

def _parse_entry(lines, start_index):
    lender = ""
    product = ""
    date = ""
    amounts = [None] * 4

    if RE_DATE.match(lines[start_index]):
        line = lines[start_index]
        date = line.split()[0]
        amounts = _collect_from_same_line(line)

    j = start_index - 1
    while j >= 0 and _is_upper(lines[j]):
        if not lender:
            lender = lines[j]
        else:
            lender = lines[j] + " " + lender
        j -= 1

    if j >= 0 and not _is_upper(lines[j]):
        product = lines[j]

    return {
        "Źródło": "auto",
        "Rodzaj_produktu": product.strip(),
        "Kredytodawca": lender.strip(),
        "Zawarcie_umowy": date,
        "Pierwotna_kwota": amounts[0],
        "Pozostało_do_spłaty": amounts[1],
        "Kwota_raty": amounts[2],
        "Suma_zaległości": amounts[3],
    }

def parse_bik_pdf(pdf_bytes: bytes, source="auto"):
    lines = _slice_active(_read_lines(pdf_bytes))
    rows = []
    for i, l in enumerate(lines):
        if RE_DATE.search(l):
            row_data = _parse_entry(lines, i)
            rows.append(row_data)
    return rows
