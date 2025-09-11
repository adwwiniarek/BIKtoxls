import fitz, re

RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO   = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL  = re.compile(r"^Łącznie\b", re.I)
RE_DATE   = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_ANYD   = re.compile(r"\d{2}\.\d{2}\.\d{4}")
RE_FORBID = re.compile(r"(PLN|ND|BRAK|\d)")

AMOUNT_RE = re.compile(r"(ND|BRAK|(?:\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?\s*PLN))", re.I)

def _read_lines(pdf_bytes: bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    lines = []
    for p in doc:
        lines += [l.strip() for l in p.get_text("text").splitlines() if l.strip()]
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

def _parse_tok(tok: str, pos: int):
    up = tok.upper().strip()
    if up == "ND":   return None
    if up == "BRAK": return 0.0 if pos == 3 else None
    t = up.replace("PLN","").replace("\u00a0"," ").replace(" ","")
    t = t.replace(".","").replace(",",".")
    try: return float(t)
    except: return None

def _collect4_strict(lines, start_idx, hard_limit=30):
    tokens = []
    i = start_idx
    stop_words = ("Historia spłaty", "Relacja", "Zgoda indywidualna", "Postęp w spłacie")
    while i < len(lines) and (i - start_idx) < hard_limit:
        ln = lines[i].strip()
        if any(sw in ln for sw in stop_words):
            break
        if RE_DATE.match(ln) and i > start_idx:
            break  # kolejny rekord
        for m in AMOUNT_RE.finditer(ln):
            tokens.append(m.group(0))
            if len(tokens) == 4: break
        if len(tokens) == 4: break
        i += 1
    tokens = tokens[:4] + [None]*(4 - len(tokens))
    return [_parse_tok(t, idx) if t else None for idx, t in enumerate(tokens)]

def parse_bik_pdf(pdf_bytes: bytes, source="auto"):
    lines = _slice_active(_read_lines(pdf_bytes))
    rows = []
    for i,l in enumerate(lines):
        if RE_DATE.search(l):
            data = l.split()[0]
            lender, j = _lender_block(lines, i)
            product   = _product_above(lines, j)
            k1,k2,k3,k4 = _collect4_strict(lines, i, hard_limit=30)
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
