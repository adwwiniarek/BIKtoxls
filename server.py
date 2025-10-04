from flask import Flask, request, jsonify, send_file
from parse_bik import parse_bik_pdf
import pandas as pd
import io
import logging

# Konfiguracja loggera, aby widzieć błędy na Render
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

@app.route('/process_bik', methods=['POST'])
def process_bik_report():
    """
    Endpoint, który przyjmuje plik PDF, parsuje go i zwraca plik XLSX.
    """
    logging.info("Otrzymano nowe żądanie do /process_bik")

    if 'file' not in request.files:
        logging.error("Brak pliku w żądaniu.")
        return jsonify({"error": "Brak pliku w żądaniu."}), 400

    file = request.files['file']
    source = request.form.get('source', 'auto') # Pobiera źródło z formularza

    if file.filename == '':
        logging.error("Nie wybrano pliku.")
        return jsonify({"error": "Nie wybrano pliku."}), 400

    try:
        pdf_bytes = file.read()
        logging.info(f"Odczytano {len(pdf_bytes)} bajtów z pliku '{file.filename}'. Źródło: {source}")
        
        # Uruchomienie parsera
        parsed_data = parse_bik_pdf(pdf_bytes, source)
        
        if not parsed_data:
            logging.warning("Parser nie zwrócił żadnych danych.")
            # Zwróć pusty plik excel, aby uniknąć błędu w Notion
            df = pd.DataFrame()
        else:
            logging.info(f"Parser zwrócił {len(parsed_data)} rekordów.")
            df = pd.DataFrame(parsed_data)

        # Stworzenie pliku Excel w pamięci
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Zobowiazania')
        
        output.seek(0)

        logging.info("Pomyślnie utworzono plik Excel. Odsyłanie odpowiedzi.")
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='raport_wynikowy.xlsx'
        )

    except Exception as e:
        logging.error(f"Wystąpił krytyczny błąd podczas przetwarzania: {e}", exc_info=True)
        return jsonify({"error": f"Wystąpił wewnętrzny błąd serwera: {e}"}), 500

if __name__ == '__main__':
    # Ta sekcja jest do testów lokalnych, nie jest używana przez Gunicorn na Render
    app.run(debug=True, port=5001)
