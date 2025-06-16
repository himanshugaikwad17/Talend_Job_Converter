import os
import re
import json
import requests
from extract_graph import extract_from_internal_components

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "scout"


def parse_dag_tasks(dag_path: str):
    """Return list of task_ids from an Airflow DAG file."""
    tasks = []
    task_id_re = re.compile(r"task_id\s*=\s*['\"]([^'\"]+)['\"]")
    with open(dag_path, "r", encoding="utf-8") as f:
        for line in f:
            m = task_id_re.search(line)
            if m:
                tasks.append(m.group(1))
    return tasks


def call_groq(prompt: str) -> str:
    """Call Groq API with given prompt and return response text."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY environment variable not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "user", "content": prompt},
        ],
    }
    resp = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()


def build_prompt(job_steps, dag_tasks):
    job_json = json.dumps(job_steps, indent=2)
    dag_json = json.dumps(dag_tasks, indent=2)
    return (
        "Compare this Talend job and this Airflow DAG and list any missing steps.\n"
        f"Talend steps:\n{job_json}\n\nAirflow tasks:\n{dag_json}"
    )


def main(job_file: str, dag_file: str):
    if not os.path.exists(job_file):
        raise FileNotFoundError(job_file)
    if not os.path.exists(dag_file):
        raise FileNotFoundError(dag_file)

    nodes, _ = extract_from_internal_components(job_file)
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
