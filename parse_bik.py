import fitz, re

RE_ACTIVE = re.compile(r"Zobowiązania\s+finansowe\s*-\s*w\s*trakcie\s*spłaty", re.I)
RE_CLOSED = re.compile(r"Zobowiązania\s+finansowe\s*-\s*zamknięte", re.I)
RE_INFO   = re.compile(r"Informacje\s+dodatkowe|Informacje\s+szczegółowe", re.I)
RE_TOTAL  = re.compile(r"^Łącznie\b", re.I)
RE_DATE   = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\b")
RE_ANYD   = re.compile(r"\d{2}\.\d{2}\.\d{4}")

# cokolwiek zawiera PLN/ND/BRAK lub cyfry – to nie jest czysta etykieta
RE_FORBID = re.compile(r"(PLN|ND|BRAK|\d)")

# słowa-klucze produktu (również gdy są WIELKIMI LITERAMI)
RE_PRODUCT_HINT = re.compile(
    r"\b(kredyt|pożyczka|gotówkowy|konsolidacyjny|hipoteczny|obrotowy|"
    r"rachunku|limit|debetowy|karta|leasing|faktoring|linia)\b", re.I
)

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

def _is_all_caps_lender_line(line: str) -> bool:
    # wielkie litery, bez kwot/dat, i NIE wygląda jak nazwa produktu
    if RE_FORBID.search(line) or RE_ANYD.search(line): return False
    if RE_PRODUCT_HINT.search(line): return False
    letters = re.sub(r"[^A-Za-zĄąĆćĘęŁłŃńÓóŚśŹźŻż]", "", line)
    return bool(letters) and line == line.upper()

def _find_product_and_lender(lines, date_idx, max_back=8):
    """
    Szukamy w górę od wiersza z datą:
      - najpierw wiersza z produktem (po słowach-kluczach),
      - potem zbieramy ciąg WIELKICH liter pomiędzy produktem a datą jako wierzyciela.
    Jeśli nie znajdziemy produktu, fallback do najbliższego „caps bloku” powyżej.
    """
    product = ""
    lender_lines = []
    # 1) produkt
    j = date_idx - 1
    while j >= 0 and date_idx - j <= max_back:
        ln = lines[j]
        if not RE_FORBID.search(ln) and RE_PRODUCT_HINT.search(ln):
            product = ln.strip()
            # 2) wierzyciel – ciąg caps między produktem a datą
            k = j + 1
            while k < date_idx and _is_all_caps_lender_line(lines[k]):
                lender_lines.append(lines[k].strip())
                k += 1
            break
        j -= 1

    # fallback – gdy produkt nie trafiony, spróbuj „starym” sposobem
    if not product:
        # caps-blok tuż nad datą to wierzyciel
        k = date_idx - 1
        while k >= 0 and _is_all_caps_lender_line(lines[k]):
            lender_lines.insert(0, lines[k].strip())
            k -= 1
        # produkt: pierwszy sensowny wiersz nad caps-blokiem z hintem
        if k >= 0 and RE_PRODUCT_HINT.search(lines[k]):
            product = lines[k].strip()

    lender = re.sub(r"\s+", " ", " ".join(lender_lines)).strip()
    product = re.sub(r"\s+", " ", product).strip()
    return product, lender

def _parse_tok(tok, pos):
    up = tok.upper().strip()
    if up == "ND":   return None
    if up == "BRAK": return 0.0 if pos == 3 else None
    t = up.replace("PLN","").replace("\u00a0"," ").replace(" ","")
    t = t.replace(".","").replace(",",".")
    try: return float(t)
    except: return None

def _collect4_from_date_line(lines, idx):
    """
    Kwoty bierzemy z wiersza z datą + ewentualnie następny wiersz,
    gdy PDF „zawinął” fragment.
    """
    import re as _re
    chunk = lines[idx]
    if idx + 1 < len(lines) and not RE_DATE.search(lines[idx+1]):
        chunk += " " + lines[idx+1]
    toks = _re.findall(r"\d{1,3}(?:[\.\s]\d{3})*(?:,\d+)?\s*PLN|ND|BRAK", chunk, _re.I)
    toks = toks[:4] + [None]*(4-len(toks))
    return [_parse_tok(t, i) if t else None for i,t in enumerate(toks)]

def parse_bik_pdf(pdf_bytes: bytes, source="auto"):
    lines = _slice_active(_read_lines(pdf_bytes))
    rows = []
    for i,l in enumerate(lines):
        if RE_DATE.search(l):
            data = l.split()[0]
            product, lender = _find_product_and_lender(lines, i)
            k1,k2,k3,k4 = _collect4_from_date_line(lines, i)

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
