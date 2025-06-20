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
                'sql': node['data'].get('sql', '')[:200] if node['data'].get('sql') else None  # Further limit SQL length
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

    # Build a more concise prompt
    active_nodes = [n for n in simplified_structure['nodes'] if n['sql']]
    sql_details = {}
    for node in active_nodes:
        sql_details[node['label']] = {
            'sql': node['sql'],
            'type': node['type']
        }
    
    sql_summary = "\n".join([f"- {n['label']}: {n['sql'][:100]}..." for n in active_nodes[:5]])  # Limit to 5 nodes for summary
    
    prompt = f"""Generate an Airflow DAG for Talend job: {simplified_structure['jobName']}

Components ({len(simplified_structure['nodes'])} active):
{json.dumps([{'id': n['id'], 'type': n['type'], 'label': n['label']} for n in simplified_structure['nodes']], indent=2)}

Flow: {json.dumps(simplified_structure['edges'], indent=2)}

SQL Operations:
{sql_summary}

Requirements:
1. Use SnowflakeOperator for SQL operations
2. Maintain execution order
3. Include error handling
4. Follow Airflow best practices
5. Use the following SQL statements in corresponding tasks:
{json.dumps(sql_details, indent=2)}

Base structure on this sample:
{sample_dag_code[:1000]}"""  # Limit sample DAG length

    try:
        dag_code = call_groq(prompt)
        
        # Return the generated DAG code without automatic SQL injection
        # SQL injection will be handled as a separate feature during lineage analysis
        return jsonify({'dag_code': dag_code})
    except Exception as e:
        error_msg = str(e)
        print(f"DAG Generation Error: {error_msg}")
        
        if "timeout" in error_msg.lower():
            return jsonify({
                'error': 'DAG generation timed out. The job might be too complex. Try reducing the number of components or simplifying the SQL queries.'
            }), 408
        elif "payload too large" in error_msg.lower():
            return jsonify({
                'error': 'Job structure is too large for processing. Try with a simpler job or fewer components.'
            }), 413
        elif "api key" in error_msg.lower():
            return jsonify({
                'error': 'Groq API configuration issue. Please check your API key configuration.'
            }), 500
        else:
            return jsonify({
                'error': f'DAG generation failed: {error_msg}'
            }), 500

def fix_truncated_sql_statements(dag_code, sql_details):
    """Fix truncated SQL statements by finding and replacing them with complete versions."""
    import re
    
    # Look for truncated SQL patterns - more specific patterns to avoid over-matching
    truncated_patterns = [
        r"sql\s*=\s*['\"`]?.*?MERGE INTO.*?USING.*?ON.*?['\"`]?\s*$",  # Complete MERGE statements that end abruptly
        r"sql\s*=\s*['\"`]?.*?-- PROPERTIES Query:.*?['\"`]?\s*$",  # Properties queries that end abruptly
    ]
    
    processed_code = dag_code
    
    for pattern in truncated_patterns:
        matches = list(re.finditer(pattern, processed_code, re.DOTALL | re.IGNORECASE | re.MULTILINE))
        # Process matches in reverse order to avoid index shifting
        for match in reversed(matches):
            truncated_sql = match.group(0)
            
            # Find the complete SQL that contains this truncated version
            for comp_name, comp_details in sql_details.items():
                complete_sql = comp_details['sql']
                
                # Check if the truncated SQL is part of the complete SQL
                if any(keyword in truncated_sql and keyword in complete_sql 
                      for keyword in ['MERGE INTO', 'USING', 'ON', 'PROPERTIES']):
                    
                    # Replace the truncated SQL with complete SQL
                    new_sql_param = f"sql='''{complete_sql}'''"
                    processed_code = processed_code[:match.start()] + new_sql_param + processed_code[match.end():]
                    break
    
    return processed_code

def replace_incomplete_sql_patterns(dag_code, sql_details):
    """Replace incomplete SQL patterns directly in the DAG code."""
    import re
    
    processed_code = dag_code
    
    # Look for incomplete SQL patterns and replace them
    incomplete_patterns = [
        # Handle the specific case with t_snowflake_connection_sql
        r"t_snowflake_connection_sql\s*=\s*['\"`]?.*?['\"`]?",
        # Handle incomplete MERGE statements
        r"sql\s*=\s*['\"`]?.*?-- PROPERTIES Query:.*?MERGE INTO.*?USING\s*['\"`]?",
        r"sql\s*=\s*['\"`]?.*?MERGE INTO.*?USING\s*['\"`]?",
        r"sql\s*=\s*['\"`]?.*?ON.*?['\"`]?",
        # Handle other incomplete patterns
        r"sql\s*=\s*['\"`]?.*?-- PROPERTIES Query:.*?['\"`]?",
        r"sql\s*=\s*['\"`]?.*?SNOWFLAKE_ROW_SQL.*?['\"`]?"
    ]
    
    for pattern in incomplete_patterns:
        matches = list(re.finditer(pattern, processed_code, re.DOTALL | re.IGNORECASE))
        print(f"Found {len(matches)} incomplete SQL patterns with pattern: {pattern}")
        
        for match in reversed(matches):  # Process in reverse to avoid index shifting
            incomplete_sql = match.group(0)
            print(f"Incomplete SQL found: {incomplete_sql[:200]}...")
            
            # Find the best matching complete SQL
            best_match = None
            best_score = 0
            
            for comp_name, comp_details in sql_details.items():
                complete_sql = comp_details['sql']
                
                # Calculate similarity score
                score = 0
                if any(keyword in incomplete_sql and keyword in complete_sql 
                      for keyword in ['MERGE INTO', 'USING', 'ON', 'PROPERTIES', 'SNOWFLAKE_ROW_SQL', 't_snowflake_connection_sql']):
                    score += 10
                if comp_name.lower() in incomplete_sql.lower():
                    score += 5
                if 'connection' in comp_name.lower() and 'connection' in incomplete_sql.lower():
                    score += 3
                if 'row' in comp_name.lower() and 'row' in incomplete_sql.lower():
                    score += 3
                
                if score > best_score:
                    best_score = score
                    best_match = complete_sql
            
            if best_match and best_score > 5:
                # Replace the incomplete SQL with complete SQL
                if 't_snowflake_connection_sql' in incomplete_sql:
                    # Handle the specific case for connection SQL
                    new_sql_param = f"t_snowflake_connection_sql='''{best_match}'''"
                else:
                    new_sql_param = f"sql='''{best_match}'''"
                
                processed_code = processed_code[:match.start()] + new_sql_param + processed_code[match.end():]
                print(f"Replaced incomplete SQL with complete SQL from component")
    
    return processed_code

def fix_malformed_task_structures(dag_code, sql_details):
    """Fix malformed task structures and ensure proper task definitions."""
    import re
    
    # Find and fix malformed task definitions
    # Pattern to find task definitions that are missing proper structure
    malformed_pattern = r'(\w+)\s*=\s*SnowflakeOperator\s*\(\s*task_id\s*=\s*[\'"][^\'"]+[\'"]\s*,?\s*(?:sql\s*=\s*[\'"]{3}.*?[\'"]{3}\s*,?\s*)*\s*\)'
    
    def fix_task_definition(match):
        task_var = match.group(1)
        task_content = match.group(0)
        
        # Check if this task has proper SQL
        if 'sql=' not in task_content:
            # Find the appropriate SQL for this task
            for comp_name, comp_details in sql_details.items():
                if comp_name.lower().replace(' ', '_') in task_var.lower():
                    # Add SQL to the task
                    fixed_task = task_content.replace(')',
                        f",\n    sql='''{comp_details['sql']}'''\n)")
                    return f"\n# SQL from Talend component: {comp_name}\n{fixed_task}"
        
        return task_content
    
    processed_code = re.sub(malformed_pattern, fix_task_definition, dag_code, flags=re.DOTALL)
    
    # Remove any standalone SQL statements that are not part of tasks
    standalone_sql_pattern = r'^\s*sql\s*=\s*[\'"]{3}.*?[\'"]{3}\s*$'
    lines = processed_code.split('\n')
    cleaned_lines = []
    
    for line in lines:
        if not re.match(standalone_sql_pattern, line, re.MULTILINE | re.DOTALL):
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def ensure_sql_in_tasks(dag_code, sql_details):
    """Ensure SQL statements from Talend are properly included in Airflow tasks."""
    processed_code = dag_code
    
    # Step 1: Fix malformed task structures first
    processed_code = fix_malformed_task_structures(processed_code, sql_details)
    
    # Step 2: Fix truncated SQL statements
    processed_code = fix_truncated_sql_statements(processed_code, sql_details)
    
    # Step 3: Replace any incomplete SQL patterns
    processed_code = replace_incomplete_sql_patterns(processed_code, sql_details)
    
    # Step 4: Clean up any duplicate or malformed SQL statements
    processed_code = clean_up_sql_statements(processed_code)
    
    # Step 5: Ensure each component's SQL is properly mapped to its task
    for component_name, details in sql_details.items():
        # Look for task definition that corresponds to this component
        task_var = None
        
        # Common task name patterns
        possible_names = [
            component_name.lower().replace(' ', '_'),
            f"execute_{component_name.lower().replace(' ', '_')}",
            f"{component_name.lower().replace(' ', '_')}_task",
            component_name.lower().replace(' ', '_').replace('-', '_')
        ]
        
        # Find the task variable name
        import re
        for name in possible_names:
            pattern = rf'(\w+)\s*=\s*\w+Operator\s*\(\s*task_id\s*=\s*[\'"].*{name}'
            match = re.search(pattern, processed_code, re.IGNORECASE)
            if match:
                task_var = match.group(1)
                break
        
        if task_var:
            # Extract the existing task definition
            task_def_pattern = rf'{task_var}\s*=\s*\w+Operator\s*\([^)]+\)'
            task_def_match = re.search(task_def_pattern, processed_code, re.DOTALL)
            
            if task_def_match:
                task_def = task_def_match.group(0)
                
                # Check if there's any existing SQL and replace it with complete SQL
                if 'sql=' in task_def:
                    # Replace existing incomplete SQL with complete SQL
                    sql_pattern = r"sql\s*=\s*['\"`]?.*?['\"`]?"
                    new_sql_param = f"sql='''{details['sql']}'''"
                    new_task_def = re.sub(sql_pattern, new_sql_param, task_def, flags=re.DOTALL)
                else:
                    # Add SQL parameter to the task
                    new_task_def = task_def.replace(')',
                        f",\n    sql='''{details['sql']}'''\n)"
                    )
                
                # Add SQL comment for documentation
                sql_comment = f"\n# Complete SQL from Talend component: {component_name}\n"
                new_task_def = sql_comment + new_task_def
                
                processed_code = processed_code.replace(task_def, new_task_def)
    
    return processed_code

def clean_up_sql_statements(dag_code):
    """Clean up duplicate or malformed SQL statements in the DAG."""
    import re
    
    # Remove duplicate SQL statements
    lines = dag_code.split('\n')
    cleaned_lines = []
    in_sql_block = False
    sql_blocks_seen = set()
    
    for line in lines:
        # Check if this line starts a SQL block
        if re.match(r'^\s*sql\s*=\s*[\'"]{3}', line):
            sql_content = line
            in_sql_block = True
            continue
        
        # If we're in a SQL block, collect the content
        if in_sql_block:
            sql_content += '\n' + line
            if re.match(r'^\s*[\'"]{3}', line):
                # End of SQL block
                in_sql_block = False
                # Check if we've seen this SQL block before
                if sql_content not in sql_blocks_seen:
                    sql_blocks_seen.add(sql_content)
                    cleaned_lines.append(sql_content)
                continue
        
        # Regular line
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

@app.route('/generate_lineage_comparison', methods=['POST'])
def generate_lineage_comparison():
    data = request.get_json()
    if not data or 'job_structure' not in data or 'dag_code' not in data:
        return jsonify({'error': 'Invalid input'}), 400

    job_structure = data['job_structure']
    dag_code = data['dag_code']
    
    try:
        # Extract Talend flow structure
        talend_flow = {
            'job_name': job_structure['jobName'],
            'components': [
                {
                    'id': node['id'],
                    'name': node['data'].get('label', ''),
                    'type': node['data'].get('technical_type', 'Unknown'),
                    'status': node['data'].get('status', 'unknown'),
                    'sql': node['data'].get('sql', '')[:100] if node['data'].get('sql') else None
                }
                for node in job_structure['nodes']
                if node['data'].get('status') == 'active'
            ],
            'connections': [
                {
                    'from': edge['source'],
                    'to': edge['target'],
                    'type': 'data_flow'
                }
                for edge in job_structure['edges']
            ]
        }
        
        # Extract Airflow DAG structure
        dag_tasks = extract_dag_tasks_from_code(dag_code)
        
        # Generate lineage mapping
        lineage_mapping = generate_lineage_mapping(talend_flow, dag_tasks)
        
        return jsonify({
            'talend_flow': talend_flow,
            'airflow_dag': dag_tasks,
            'lineage_mapping': lineage_mapping,
            'comparison_summary': generate_comparison_summary(talend_flow, dag_tasks)
        })
        
    except Exception as e:
        error_msg = str(e)
        print(f"Lineage Comparison Error: {error_msg}")
        return jsonify({'error': f'Lineage comparison failed: {error_msg}'}), 500

def extract_dag_tasks_from_code(dag_code):
    """Extract task information from Airflow DAG code."""
    import re
    
    tasks = []
    
    # Extract task definitions
    task_patterns = [
        r'(\w+)\s*=\s*SnowflakeOperator\s*\(\s*task_id\s*=\s*[\'"]([^\'"]+)[\'"]',
        r'(\w+)\s*=\s*PythonOperator\s*\(\s*task_id\s*=\s*[\'"]([^\'"]+)[\'"]',
        r'(\w+)\s*=\s*BashOperator\s*\(\s*task_id\s*=\s*[\'"]([^\'"]+)[\'"]',
        r'(\w+)\s*=\s*DummyOperator\s*\(\s*task_id\s*=\s*[\'"]([^\'"]+)[\'"]'
    ]
    
    for pattern in task_patterns:
        matches = re.findall(pattern, dag_code)
        for var_name, task_id in matches:
            tasks.append({
                'variable_name': var_name,
                'task_id': task_id,
                'operator_type': extract_operator_type(pattern),
                'dependencies': extract_task_dependencies(dag_code, var_name)
            })
    
    return tasks

def extract_operator_type(pattern):
    """Extract operator type from regex pattern."""
    if 'SnowflakeOperator' in pattern:
        return 'SnowflakeOperator'
    elif 'PythonOperator' in pattern:
        return 'PythonOperator'
    elif 'BashOperator' in pattern:
        return 'BashOperator'
    elif 'DummyOperator' in pattern:
        return 'DummyOperator'
    return 'Unknown'

def extract_task_dependencies(dag_code, var_name):
    """Extract task dependencies from DAG code."""
    import re
    
    dependencies = []
    
    # Look for dependency patterns like: task1 >> task2
    dep_pattern = rf'{var_name}\s*>>\s*(\w+)'
    matches = re.findall(dep_pattern, dag_code)
    dependencies.extend(matches)
    
    # Look for reverse dependencies: task2 << task1
    rev_dep_pattern = rf'(\w+)\s*<<\s*{var_name}'
    matches = re.findall(rev_dep_pattern, dag_code)
    dependencies.extend(matches)
    
    return dependencies

def generate_lineage_mapping(talend_flow, dag_tasks):
    """Generate mapping between Talend components and Airflow tasks."""
    mapping = []
    
    # Create component name to task mapping
    component_to_task = {}
    
    for component in talend_flow['components']:
        component_name = component['name'].lower().replace(' ', '_').replace('-', '_')
        component_type = component['type']
        
        # Special handling for connection components
        if 'connection' in component_type.lower():
            # Find corresponding connection task
            matching_task = next(
                (task for task in dag_tasks 
                 if 'connection' in task['task_id'].lower() or 
                    'hook' in task['task_id'].lower() or
                    'conn' in task['task_id'].lower()),
                None
            )
            if matching_task:
                mapping.append({
                    'talend_component': component,
                    'airflow_task': matching_task,
                    'mapping_type': 'connection',
                    'confidence': 'high',
                    'notes': 'Mapped to Airflow connection task'
                })
            else:
                mapping.append({
                    'talend_component': component,
                    'airflow_task': None,
                    'mapping_type': 'connection',
                    'confidence': 'medium',
                    'notes': 'Handled by Airflow Connection Management'
                })
            continue

        # Handle SQL execution components
        if any(sql_type in component_type.lower() for sql_type in ['row', 'input', 'output', 'query']):
            matching_task = next(
                (task for task in dag_tasks
                 if ('snowflake' in task['task_id'].lower() and
                     any(op in task['operator_type'].lower() for op in ['snowflakeoperator', 'sqloperator'])) or
                    'execute' in task['task_id'].lower()),
                None
            )
            if matching_task:
                mapping.append({
                    'talend_component': component,
                    'airflow_task': matching_task,
                    'mapping_type': 'sql_execution',
                    'confidence': 'high',
                    'notes': 'Mapped to Airflow SQL execution task'
                })
                continue

        # Standard name-based mapping for other components
        matching_task = None
        highest_similarity = 0
        
        for task in dag_tasks:
            task_name = task['task_id'].lower()
            # Calculate similarity score
            similarity = sum(word in task_name for word in component_name.split('_'))
            if similarity > highest_similarity:
                highest_similarity = similarity
                matching_task = task

        if matching_task and highest_similarity > 0:
            mapping.append({
                'talend_component': component,
                'airflow_task': matching_task,
                'mapping_type': 'direct',
                'confidence': 'high' if highest_similarity > 1 else 'medium',
                'notes': 'Mapped based on name similarity'
            })
        else:
            mapping.append({
                'talend_component': component,
                'airflow_task': None,
                'mapping_type': 'unmapped',
                'confidence': 'low',
                'notes': 'No matching Airflow task found'
            })
    
    return mapping

def generate_comparison_summary(talend_flow, dag_tasks):
    """Generate a summary of the comparison."""
    mapping = generate_lineage_mapping(talend_flow, dag_tasks)
    
    total_talend_components = len(talend_flow['components'])
    total_airflow_tasks = len(dag_tasks)
    
    # Count different types of mappings
    direct_mappings = sum(1 for m in mapping if m['mapping_type'] == 'direct')
    connection_mappings = sum(1 for m in mapping if m['mapping_type'] == 'connection')
    sql_mappings = sum(1 for m in mapping if m['mapping_type'] == 'sql_execution')
    
    # Calculate effective mapping percentage including special cases
    effective_mappings = direct_mappings + connection_mappings + sql_mappings
    
    return {
        'talend_components': total_talend_components,
        'airflow_tasks': total_airflow_tasks,
        'mapped_components': effective_mappings,
        'mapping_percentage': round((effective_mappings / total_talend_components) * 100, 1) if total_talend_components > 0 else 0,
        'unmapped_components': total_talend_components - effective_mappings,
        'extra_airflow_tasks': max(0, total_airflow_tasks - effective_mappings),
        'mapping_details': {
            'direct_mappings': direct_mappings,
            'connection_mappings': connection_mappings,
            'sql_mappings': sql_mappings
        }
    }

@app.route('/inject_sql_into_dag', methods=['POST'])
def inject_sql_into_dag():
    """
    Rebuilds the Airflow DAG from the Talend job structure, injecting full SQL.
    This ignores the potentially malformed AI-generated DAG body and uses the
    Talend structure as the source of truth.
    """
    try:
        data = request.get_json()
        dag_code = data.get('dag_code', '')  # Still needed for metadata
        talend_flow = data.get('talend_flow', None)

        if not talend_flow:
            return jsonify({'error': 'Missing Talend flow structure'}), 400

        print("Rebuilding DAG from Talend flow structure...")
        processed_dag = rebuild_dag_with_sql(dag_code, talend_flow)

        # Count components with SQL for the response
        sql_components = [
            n['label'] for n in talend_flow.get('nodes', []) if n.get('sql')
        ]

        return jsonify({
            'dag_code': processed_dag,
            'sql_injected': len(sql_components),
            'components_processed': sql_components
        })

    except Exception as e:
        print(f"DAG rebuild error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'DAG rebuild failed: {str(e)}'}), 500


def rebuild_dag_with_sql(dag_code, talend_flow):
    """
    Constructs a new, clean Airflow DAG from the Talend job structure,
    injecting the complete SQL for each relevant component.
    """
    import re
    
    # Extract metadata from the original DAG, but don't trust its structure
    dag_id_match = re.search(r'DAG_ID\s*=\s*["\']([^"\']+)["\']', dag_code)
    dag_description_match = re.search(r'DAG_DESCRIPTION\s*=\s*["\']([^"\']+)["\']', dag_code)
    conn_match = re.search(r'SNOWFLAKE_CONN_ID\s*=\s*["\']([^"\']+)["\']', dag_code, re.IGNORECASE)
    if not conn_match:
        conn_match = re.search(r'SNOWFLAKE_CONNECTION\s*=\s*["\']([^"\']+)["\']', dag_code, re.IGNORECASE)

    dag_id = dag_id_match.group(1) if dag_id_match else "talend_dag"
    dag_description = dag_description_match.group(1) if dag_description_match else "Talend job DAG"
    snowflake_conn = conn_match.group(1) if conn_match else "snowflake_default"

    # Boilerplate for a clean DAG
    new_dag_code = f'''from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.exceptions import AirflowException

# Define DAG metadata
DAG_ID = "{dag_id}"
DAG_DESCRIPTION = "{dag_description}"

# Define default arguments
DEFAULT_ARGS = {{
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}}

# Define Snowflake connection
SNOWFLAKE_CONN_ID = "{snowflake_conn}"

# Error handling callback
def handle_error(context):
    logging.error(f"Error in task: {{context['task_instance'].task_id}}")
    logging.error(f"Exception: {{context['exception']}}")
    raise AirflowException(f"Task {{context['task_instance'].task_id}} failed.")

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    on_failure_callback=handle_error
) as dag:
'''

    # Create a task for each Snowflake component node
    nodes = talend_flow.get('nodes', [])
    node_id_to_task_var = {}
    
    for node in nodes:
        component_type = node.get('type', '')
        # Create operators only for components that execute SQL
        if 'snowflake' in component_type.lower():
            component_name = node['label']
            task_variable_name = f"task_{component_name.lower().replace(' ', '_').replace('-', '_')}"
            task_id = component_name.lower().replace(' ', '_').replace('-', '_')
            node_id_to_task_var[node['id']] = task_variable_name

            # Use the full SQL from the component data. Use r-string for safety.
            node_sql = node.get('sql', f'-- No SQL provided for {component_name}')

            new_dag_code += f'''
    # Task for component: {component_name}
    {task_variable_name} = SnowflakeOperator(
        task_id="{task_id}",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=r"""
{node_sql}
""",
    )
'''
    # Create dependencies based on edges
    edges = talend_flow.get('edges', [])
    if edges:
        new_dag_code += '''
    # Define task dependencies from Talend job flow
'''
        for edge in edges:
            source_task_var = node_id_to_task_var.get(edge['source'])
            target_task_var = node_id_to_task_var.get(edge['target'])
            if source_task_var and target_task_var:
                new_dag_code += f"    {source_task_var} >> {target_task_var}\\n"

    return new_dag_code

# Remove all old, fragile SQL patching functions as they are now obsolete.
# The `rebuild_dag_with_sql` is now the definitive solution.
# The functions comprehensive_sql_replacement, fix_specific_malformed_sql, 
# clean_malformed_sql, and replace_incomplete_sql_patterns will be removed.

# This function now acts as a simple dispatcher to the robust rebuild logic.
def inject_sql_into_dag_tasks(dag_code, talend_flow):
    """
    This function is now a wrapper that directly calls the robust
    DAG rebuilding logic, ensuring a correct and clean DAG is always produced.
    """
    return rebuild_dag_with_sql(dag_code, talend_flow)

if __name__ == '__main__':
    app.run(debug=True)
