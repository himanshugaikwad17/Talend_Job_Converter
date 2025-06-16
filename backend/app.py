from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile
import os
from extract_graph import extract_from_internal_components

app = Flask(__name__)
CORS(app)

@app.route('/upload', methods=['POST'])
def upload_item_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    with tempfile.NamedTemporaryFile(delete=False, suffix=".item") as temp:
        uploaded_file.save(temp.name)
        temp_path = temp.name

    try:
        nodes, edges = extract_from_internal_components(temp_path)
        return jsonify({'nodes': nodes, 'edges': edges})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        os.remove(temp_path)

if __name__ == '__main__':
    app.run(debug=True)
