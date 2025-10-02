# parse_bik.py (wersja diagnostyczna)
import fitz
import unicodedata
import re

def _deep_clean_text(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    s = re.sub(r'[\u2010-\u2015]', '-', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def diagnose_pdf_text(pdf_bytes: bytes) -> str:
    """
    Funkcja diagnostyczna, która tylko wyciąga surowy tekst z PDF,
    nie próbuje go w żaden sposób interpretować.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        all_extracted_text = []
        
        if doc.is_encrypted:
            return "DIAGNOZA: BŁĄD - Plik PDF jest zaszyfrowany i nie można go odczytać."
            
        for page_num, page in enumerate(doc, 1):
            all_extracted_text.append(f"\n--- STRONA {page_num} ---\n")
            # Używamy zaawansowanej metody "blocks"
            blocks = page.get_text("blocks", sort=True)
            
            if not blocks:
                all_extracted_text.append("[NA TEJ STRONIE NIE ZNALEZIONO ŻADNYCH BLOKÓW TEKSTOWYCH]")
                continue
                
            for b in blocks:
                block_text = b[4]  # Tekst w bloku
                cleaned_line = _deep_clean_text(block_text)
                if cleaned_line:
                    all_extracted_text.append(cleaned_line)
        
        full_text = "".join(all_extracted_text)
        
        # Sprawdzenie, czy wyodrębniono jakąkolwiek sensowną ilość tekstu
        if len(full_text.strip()) < 100:
             return "DIAGNOZA: BŁĄD KRYTYCZNY - Nie udało się wyodrębnić tekstu z pliku PDF. Plik jest na 99% skanem (obrazem) i wymaga przetworzenia przez program OCR, co jest poza możliwościami tego skryptu."
        
        # Jeśli tekst został znaleziony, zwróć go w całości
        return f"DIAGNOZA: SUKCES - Odczytano tekst z PDF. Oto jego treść:\n{full_text}"

    except Exception as e:
        return f"DIAGNOZA: BŁĄD KRYTYCZNY - Wystąpił błąd podczas próby otwarcia lub odczytu pliku PDF: {str(e)}"
