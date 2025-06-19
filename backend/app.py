from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile
import os
from extract_graph import extract_generic_and_safe
from compare_job_dag import compare_job_and_dag, call_groq

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
        nodes, edges, real_edges =extract_generic_and_safe(temp_path)
        return jsonify({
                'nodes': nodes,
                'edges': edges,           # Visualization edges (real + synthetic fallback)
                'real_edges': real_edges  # Used for actual DAG generation
                      })
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

@app.route('/generate_dag', methods=['POST'])
def generate_dag():
    data = request.get_json()
    if not data or 'job_structure' not in data:
        return jsonify({'error': 'Invalid input'}), 400

    job_structure = data['job_structure']
    # Load the sample DAG as a reference
    sample_dag_path = os.path.join(os.path.dirname(__file__), '../dbt_dags/sample_weather_dag.py')
    try:
        with open(sample_dag_path, 'r', encoding='utf-8') as f:
            sample_dag_code = f.read()
    except Exception as e:
        return jsonify({'error': f'Failed to load sample DAG: {e}'}), 500

    # Build the LLM prompt
    prompt = f"""
You are an expert in Airflow and Talend migration.\n
Given the following Talend job structure (nodes, edges, component types, SQL, etc.):\n{job_structure}\n\nAnd this sample Airflow DAG for reference:\n{sample_dag_code}\n\nGenerate a complete Airflow DAG in Python that replicates the Talend job's logic, using best practices.\n"""
    try:
        dag_code = call_groq(prompt)
        return jsonify({'dag_code': dag_code})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
