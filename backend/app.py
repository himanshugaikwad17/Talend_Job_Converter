from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile
import os
from extract_graph import extract_from_internal_components
from compare_job_dag import compare_job_and_dag

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


@app.route('/compare_dag', methods=['POST'])
def compare_dag():
    data = request.get_json()
    if not data or 'job_steps' not in data or 'dag_text' not in data:
        return jsonify({'error': 'Invalid input'}), 400

    try:
        result = compare_job_and_dag(data['job_steps'], data['dag_text'])
        return jsonify({'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
