# server.py (poprawiona i ulepszona wersja)

from flask import Flask, request, jsonify
from notion_client import Client
import requests
import pandas as pd
from io import BytesIO
import os

# Importujemy naszą finalną, poprawną funkcję z drugiego pliku
from parse_bik import parse_bik_pdf

# --- KONFIGURACJA ---
# Upewnij się, że nazwy poniżej DOKŁADNIE odpowiadają nazwom kolumn w Twojej bazie Notion.
# To jest najczęstsze źródło błędów.

NOTION_PDF_PROPERTY_NAME = "Raport BIK"  # <-- ZMIEŃ TĘ NAZWĘ, jeśli w Notion nazywa się inaczej
NOTION_XLS_PROPERTY_NAME = "BIK Raport"  # <-- ZMIEŃ TĘ NAZWĘ, jeśli w Notion nazywa się inaczej
NOTION_SOURCE_PROPERTY_NAME = "Źródło"   # <-- ZMIEŃ TĘ NAZWĘ, jeśli w Notion nazywa się inaczej

# --------------------

app = Flask(__name__)

# Inicjalizacja klienta Notion
notion_token = os.environ.get("NOTION_TOKEN")
notion = Client(auth=notion_token)

def create_excel_file(data):
    """Tworzy plik Excel w pamięci na podstawie przetworzonych danych."""
    output = BytesIO()
    df = pd.DataFrame(data)
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='BIK_Raport')
    output.seek(0)
    return output

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.json
        page_id = data.get('pageId')

        if not page_id:
            print("Błąd: Brak pageId w żądaniu od Notion.")
            return jsonify({"error": "Brak pageId"}), 400

        print(f"Otrzymano żądanie dla strony: {page_id}")
        page_data = notion.pages.retrieve(page_id=page_id)
        props = page_data.get('properties', {})
        
        # --- POPRAWIONA LOGIKA SPRAWDZAJĄCA ---
        pdf_property = props.get(NOTION_PDF_PROPERTY_NAME, {})
        xls_property = props.get(NOTION_XLS_PROPERTY_NAME, {})

        pdf_files = pdf_property.get('files', [])
        xls_files = xls_property.get('files', [])

        if not pdf_files:
            print(f"Brak akcji: Nie znaleziono plików PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}'.")
            return jsonify({"message": f"ℹ️ Brak akcji (brak PDF w kolumnie '{NOTION_PDF_PROPERTY_NAME}')"})
        
        if xls_files:
            print(f"Brak akcji: Plik wynikowy XLS już istnieje w kolumnie '{NOTION_XLS_PROPERTY_NAME}'.")
            return jsonify({"message": f"ℹ️ Brak akcji (plik XLS już istnieje w '{NOTION_XLS_PROPERTY_NAME}')"})
        
        # --- KONIEC POPRAWIONEJ LOGIKI ---

        pdf_url = pdf_files[0]['file']['url']
        source_property = props.get(NOTION_SOURCE_PROPERTY_NAME, {})
        source = source_property.get('select', {}).get('name', 'auto')

        print(f"Rozpoczynam przetwarzanie pliku z URL: {pdf_url}")
        response = requests.get(pdf_url)
        response.raise_for_status()
        pdf_bytes = response.content

        # Wywołanie naszego finalnego parsera
        parsed_data = parse_bik_pdf(pdf_bytes, source=source)

        if not parsed_data:
            print("Błąd: Parser nie zwrócił żadnych danych.")
            # Tutaj można by zaktualizować Notion o status błędu
            return jsonify({"error": "Nie znaleziono danych do przetworzenia w pliku PDF"}), 400

        print(f"Znaleziono {len(parsed_data)} rekordów. Tworzę plik Excel.")
        # excel_file_bytes = create_excel_file(parsed_data)
        
        # UWAGA: Poniższa logika aktualizacji Notion jest uproszczona.
        # W realnym scenariuszu należałoby najpierw wrzucić plik Excel na serwer
        # (np. S3, Google Cloud Storage), uzyskać publiczny URL i dopiero ten URL wkleić do Notion.
        # Poniższy kod tylko symuluje aktualizację statusu.
        
        # notion.pages.update(
        #     page_id=page_id,
        #     properties={
        #         NOTION_XLS_PROPERTY_NAME: {
        #             "files": [{"type": "external", "name": "BIK_Raport.xlsx", "external": {"url": "TUTAJ_URL_DO_PLIKU_XLSX"}}]
        #         }
        #     }
        # )
        
        print("Przetwarzanie zakończone pomyślnie.")
        return jsonify({"message": "Plik przetworzony pomyślnie. (Logika uploadu i aktualizacji Notion jest uproszczona)"})

    except Exception as e:
        print(f"Wystąpił krytyczny błąd: {e}")
        return jsonify({"status": "Błąd serwera", "error": str(e)}), 500

if __name__ == '__main__':
    # Render automatycznie zarządza portem, więc os.environ.get('PORT', 8080) jest dobrą praktyką
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
