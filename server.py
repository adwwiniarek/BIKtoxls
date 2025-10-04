from flask import Flask, request, jsonify, send_file
from parse_bik import parse_bik_pdf
import pandas as pd
import io
import logging

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

def process_file_and_get_excel(pdf_bytes, source):
    """Wspólna logika do parsowania i tworzenia pliku Excel."""
    parsed_data = parse_bik_pdf(pdf_bytes, source)
    
    if not parsed_data:
        logging.warning("Parser nie zwrócił żadnych danych.")
        df = pd.DataFrame()
    else:
        logging.info(f"Parser zwrócił {len(parsed_data)} rekordów.")
        df = pd.DataFrame(parsed_data)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Zobowiazania')
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='raport_wynikowy.xlsx'
    )

# #####################################################################
# ## NOWY, GŁÓWNY ENDPOINT - ZALECANY DO UŻYTKU W PRZYSZŁOŚCI      ###
# #####################################################################
@app.route('/process_bik', methods=['POST'])
def handle_process_bik():
    logging.info("Otrzymano żądanie na NOWY endpoint /process_bik")
    if 'file' not in request.files:
        return jsonify({"error": "Brak pliku w żądaniu."}), 400
    file = request.files['file']
    source = request.form.get('source', 'auto')
    if file.filename == '':
        return jsonify({"error": "Nie wybrano pliku."}), 400
    try:
        pdf_bytes = file.read()
        return process_file_and_get_excel(pdf_bytes, source)
    except Exception as e:
        logging.error(f"Błąd w /process_bik: {e}", exc_info=True)
        return jsonify({"error": f"Wystąpił wewnętrzny błąd serwera: {str(e)}"}), 500

# #####################################################################
# ## STARY ENDPOINT ('tylne drzwi') PRZYWRÓCONY ZGODNIE Z POLECENIEM ###
# #####################################################################
@app.route('/notion/poll-one', methods=['GET', 'POST'])
def handle_notion_poll_one():
    # Ten endpoint jest przywrócony, aby zapewnić kompatybilność wsteczną.
    # Akceptuje zarówno GET (który wysyła Notion), jak i POST (który jest potrzebny do wysyłki plików).
    logging.info(f"Otrzymano żądanie na STARY endpoint /notion/poll-one metodą {request.method}")
    
    if request.method == 'POST':
        # Jeśli Notion zostanie kiedyś poprawione, aby wysyłać pliki metodą POST, ta logika zadziała.
        if 'file' not in request.files:
            return jsonify({"error": "Brak pliku w żądaniu POST."}), 400
        file = request.files['file']
        source = request.form.get('source', 'legacy_post')
        if file.filename == '':
            return jsonify({"error": "Nie wybrano pliku."}), 400
        try:
            pdf_bytes = file.read()
            return process_file_and_get_excel(pdf_bytes, source)
        except Exception as e:
            logging.error(f"Błąd w /notion/poll-one (POST): {e}", exc_info=True)
            return jsonify({"error": f"Wystąpił wewnętrzny błąd serwera: {str(e)}"}), 500
            
    elif request.method == 'GET':
        # Ta część obsługuje bieżące, błędne żądanie GET z Notion.
        # Nie może przetworzyć pliku, bo GET nie przesyła plików.
        # Zwraca prostą odpowiedź, aby uniknąć błędu 404/405.
        logging.warning("Żądanie GET na /notion/poll-one. Nie można przetworzyć pliku.")
        return jsonify({
            "status": "Odebrano żądanie GET",
            "message": "Ten endpoint działa. Aby przetworzyć plik, Twoja automatyzacja musi wysłać go metodą POST."
        }), 200

if __name__ == '__main__':
    app.run(debug=True, port=5001)
