import os
import re
import json
import requests
from extract_graph import extract_generic_and_safe

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "groq_config.json")


def load_config():
    """Load Groq API configuration from JSON file."""
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(CONFIG_PATH)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("api_key"), data.get("model", "llama3-8b-8192")


def parse_dag_tasks(dag_path: str):
    """Return list of task_ids from an Airflow DAG file."""
    with open(dag_path, "r", encoding="utf-8") as f:
        return parse_dag_tasks_from_text(f.read())


def parse_dag_tasks_from_text(text: str):
    """Return list of task_ids from Airflow DAG source text."""
    tasks = []
    task_id_re = re.compile(r"task_id\s*=\s*['\"]([^'\"]+)['\"]")
    for line in text.splitlines():
        m = task_id_re.search(line)
        if m:
            tasks.append(m.group(1))
    return tasks


def call_groq(prompt: str) -> str:
    """Call Groq API with given prompt and return response text."""
    api_key, model = load_config()
    if not api_key:
        raise RuntimeError("Groq API key not configured")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 4000,  # Limit response size
        "temperature": 0.3,  # Lower temperature for more focused responses
    }
    
    # Retry logic with exponential backoff
    max_retries = 3
    base_timeout = 60  # Increased timeout
    
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                GROQ_API_URL, 
                headers=headers, 
                json=payload, 
                timeout=base_timeout * (2 ** attempt)  # Exponential backoff: 60s, 120s, 240s
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"].strip()
            
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Groq API request timed out after {max_retries} attempts")
            print(f"Attempt {attempt + 1} timed out, retrying...")
            continue
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Groq API request failed: {str(e)}")
            print(f"Attempt {attempt + 1} failed: {str(e)}, retrying...")
            continue
    
    # This should never be reached, but just in case
    raise RuntimeError("Groq API request failed after all retry attempts")


def build_prompt(job_steps, dag_tasks):
    job_json = json.dumps(job_steps, indent=2)
    dag_json = json.dumps(dag_tasks, indent=2)
    return (
        "You are a software architect verifying Talend to Airflow DAG migration.\n"
        "Compare the Talend component list and the Airflow DAG tasks.\n"
        "Ignore connection steps like tSnowflakeConnection or tDBConnection if they are not present in the DAG.\n"
        "Point out any missing or extra steps in the DAG, and suggest if any Talend step has no Airflow equivalent.\n\n"
        f"Talend Components:\n{job_json}\n\nAirflow DAG Tasks:\n{dag_json}"
    )


def compare_job_and_dag(job_steps, dag_text):
    """Compare job step list with DAG text using Groq LLM and return result."""
    dag_tasks = parse_dag_tasks_from_text(dag_text)
    prompt = build_prompt(job_steps, dag_tasks)
    return call_groq(prompt)


def main(job_file: str, dag_file: str):
    if not os.path.exists(job_file):
        raise FileNotFoundError(job_file)
    if not os.path.exists(dag_file):
        raise FileNotFoundError(dag_file)

    # Unpack all three: nodes, edges, real_edges
    nodes, _, _ = extract_generic_and_safe(job_file)
    job_steps = [n["id"] for n in nodes]

    dag_tasks = parse_dag_tasks(dag_file)
    prompt = build_prompt(job_steps, dag_tasks)
    result = call_groq(prompt)
    print(result)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compare Talend job with Airflow DAG using Groq LLM")
    parser.add_argument("job_file", help="Path to Talend .item file")
    parser.add_argument("dag_file", help="Path to Airflow DAG Python file")
    args = parser.parse_args()

    main(args.job_file, args.dag_file)
