from flask import Flask, request, jsonify
from flask_cors import CORS
import tempfile
import os
from extract_graph import extract_generic_and_safe
from compare_job_dag import compare_job_and_dag, call_groq
import json

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
    
    # Create a simplified version of the job structure
    simplified_structure = {
        'jobName': job_structure['jobName'],
        'nodes': [
            {
                'id': node['id'],
                'type': node['data'].get('technical_type', 'Unknown'),
                'label': node['data'].get('label', ''),
                'sql': node['data'].get('sql', '')[:500] if node['data'].get('sql') else None  # Limit SQL length
            }
            for node in job_structure['nodes']
            if node['data'].get('status') == 'active'  # Only include active components
        ],
        'edges': [
            {
                'source': edge['source'],
                'target': edge['target']
            }
            for edge in job_structure['edges']
            # Only include edges where both source and target are active
            if any(node['id'] == edge['source'] and node['data'].get('status') == 'active' 
                   for node in job_structure['nodes']) and
               any(node['id'] == edge['target'] and node['data'].get('status') == 'active' 
                   for node in job_structure['nodes'])
        ]
    }

    # Load the sample DAG as a reference
    sample_dag_path = os.path.join(os.path.dirname(__file__), '../dbt_dags/sample_weather_dag.py')
    try:
        with open(sample_dag_path, 'r', encoding='utf-8') as f:
            sample_dag_code = f.read()
    except Exception as e:
        return jsonify({'error': f'Failed to load sample DAG: {e}'}), 500

    # Build a more focused prompt
    prompt = f"""Generate an Airflow DAG that replicates this Talend job:

Job Name: {simplified_structure['jobName']}

Components and Their Flow:
{json.dumps([{'id': n['id'], 'type': n['type'], 'label': n['label']} for n in simplified_structure['nodes']], indent=2)}

Flow Dependencies:
{json.dumps(simplified_structure['edges'], indent=2)}

Key SQL Operations:
{json.dumps([{'component': n['label'], 'sql': n['sql']} for n in simplified_structure['nodes'] if n['sql']], indent=2)}

Please generate a complete Airflow DAG that:
1. Uses SnowflakeOperator for SQL operations
2. Maintains the same execution order as the original job
3. Includes proper error handling and logging
4. Uses best practices for Airflow DAG design

Base the structure on this sample DAG but adapt it to the job's needs:
{sample_dag_code}"""

    try:
        dag_code = call_groq(prompt)
        return jsonify({'dag_code': dag_code})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
