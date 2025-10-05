import fitz, re

NBSP_CHARS = "\u00A0\u202F\u2009"  # NBSP, narrow no-break, thin space

def _normalize_text(s: str) -> str:
    if any(c in s for c in NBSP_CHARS):
        s = re.sub(f"[{NBSP_CHARS}]", " ", s)
    s = re.sub(r"(?<=\d)[ .](?=\d{3}\b)", "", s)
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO   = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL  = re.compile(r"^Łącznie\b", re.I)
RE_DATE   = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_ANYD   = re.compile(r"\d{2}\.\d{2}\.\d{4}")
RE_FORBID = re.compile(r"(PLN|ND|BRAK|\d)")

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

def _lender_block(lines, idx, max_up=8):
    parts, j = [], idx-1
    while j >= 0 and len(parts) < max_up and _is_upper(lines[j]):
        parts.insert(0, lines[j]); j -= 1
    lender = " ".join(parts).strip()
    return re.sub(r"\s+", " ", lender), j

def _product_above(lines, j_above):
    if j_above >= 0:
        prod = lines[j_above].strip()
        if not _is_upper(prod):
            return re.sub(r"\s+", " ", prod)
    return ""

def _parse_amount(tok: str, pos: int):
    up = tok.upper().strip()
    if up == "ND":   return None
    if up == "BRAK": return 0.0 if pos == 3 else None
    t = up.replace("PLN", "").strip().replace(",", ".")
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

def parse_bik_pdf(pdf_bytes: bytes, source="auto"):
    lines = _slice_active(_read_lines(pdf_bytes))
    rows = []
    for i,l in enumerate(lines):
        if RE_DATE.search(l):
            data = l.split()[0]
            lender, j  = _lender_block(lines, i)
            product    = _product_above(lines, j)
            k1,k2,k3,k4 = _collect_from_same_line(l)
            rows.append({
                "Źródło": source,
                "Rodzaj_produktu": product,
                "Kredytodawca": lender,
                "Zawarcie_umowy": data,
                "Pierwotna_kwota": k1,
                "Pozostało_do_spłaty": k2,
                "Kwota_raty": k3,
                "Suma_zaległości": k4,
            })
    return rows
