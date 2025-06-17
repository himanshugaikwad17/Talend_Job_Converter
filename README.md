# Talend2AirDAG

This repository contains a small React application with a Material-UI interface for visualising Talend job documentation and generating a basic Airflow DAG.

## Getting Started

Install dependencies and start the development server:

```bash
npm install
npm run dev
```

Then open the printed local URL in your browser. If uploading directories is blocked by your browser, serve your files from a local web server and open the app from that URL.

## Features

- Upload a folder of Talend HTML documentation files.
- Parse each file client-side to extract component names, SQL snippets and basic dependencies.
- Visualise the selected job using React Flow. Nodes representing `tRunJob` or joblets can be expanded.
- Click nodes to view details and any detected SQL.
- Generate a Python file containing a skeleton Airflow DAG. Database connections reference credentials from AWS Secrets Manager.
- Download a DBT-style DAG and automatically compare it with the original job using Groq's LLM.

This is a basic demo and may need adjustments for different Talend documentation formats.

## Job Comparison Reports

A helper script in `backend/compare_job_dag.py` lets you compare a Talend `.item` file with a generated Airflow DAG using Groq's `scout` model. The web app now exposes this functionality as well. Configure your API key and model in `backend/groq_config.json` then run the backend server.

```bash
python backend/compare_job_dag.py path/to/job.item path/to/dag.py
```

Install the Python dependencies first:

```bash
pip install -r backend/requirements.txt
```

The script extracts job steps and task IDs then sends them to Groq to list discrepancies or missing conversions.

