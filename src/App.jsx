import React, { useState } from 'react';
import {
  Button, List, ListItem, ListItemText,
  Drawer, AppBar, Toolbar, Typography, Box
} from '@mui/material';
import { ReactFlowProvider, ReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';

function App() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [jobName, setJobName] = useState('');
  const [selectedNode, setSelectedNode] = useState(null);

  const handleFolderUpload = async (e) => {
    const files = Array.from(e.target.files);
    const itemFile = files.find(f => f.name.endsWith('.item'));

    if (!itemFile) {
      alert("No Talend .item file found in the uploaded folder.");
      return;
    }

    const formData = new FormData();
    formData.append('file', itemFile);

    try {
      const res = await fetch('http://localhost:5000/upload', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(`Server error: ${errorText}`);
      }

      const data = await res.json();
      setNodes(data.nodes || []);
      setEdges(data.edges || []);
      setJobName(itemFile.name.replace('.item', ''));
    } catch (err) {
      console.error("Upload failed:", err);
      alert("Failed to parse job. Check console for details.");
    }
  };

  const onNodeClick = (event, node) => {
    setSelectedNode(node);
  };

  const generateDag = () => {
    if (!jobName || nodes.length === 0) return;

    const lines = [];
    lines.push("from airflow import DAG");
    lines.push("from airflow.operators.python import PythonOperator");
    lines.push("from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook");
    lines.push("from airflow.providers.odbc.hooks.odbc import OdbcHook");
    lines.push("from datetime import datetime");
    lines.push("import logging");
    lines.push("");

    lines.push("default_args = {");
    lines.push("    'owner': 'airflow',");
    lines.push("    'start_date': datetime(2023, 1, 1),");
    lines.push("}");
    lines.push("");

    lines.push(`with DAG('${jobName}', default_args=default_args, schedule_interval=None, catchup=False) as dag:`);

    nodes.forEach((n) => {
      const taskName = `task_${n.id}`;
      const label = n.data.label?.toLowerCase() || n.id;
      const sql = n.data.sql?.replace(/"""|'''/g, "") || "SELECT 1";

      if (label.includes("connection")) return;

      lines.push("");
      lines.push(`    # ${n.data.label}`);

      if (label.includes("snowflake")) {
        lines.push(`    def ${taskName}_fn():`);
        lines.push(`        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")`);
        lines.push(`        results = hook.get_records("""${sql}""")`);
        lines.push(`        logging.info("Snowflake results: %s", results)`);
      } else if (label.includes("hana")) {
        lines.push(`    def ${taskName}_fn():`);
        lines.push(`        hook = OdbcHook(odbc_conn_id="hana_default")`);
        lines.push(`        results = hook.get_records("""${sql}""")`);
        lines.push(`        logging.info("HANA results: %s", results)`);
      } else {
        lines.push(`    def ${taskName}_fn():`);
        lines.push(`        logging.info("Executing generic task: ${label}")`);
      }

      lines.push(``);
      lines.push(`    ${taskName} = PythonOperator(`);
      lines.push(`        task_id="${n.id}",`);
      lines.push(`        python_callable=${taskName}_fn`);
      lines.push(`    )`);
    });

    edges.forEach((e) => {
      lines.push(`    task_${e.source} >> task_${e.target}`);
    });

    const blob = new Blob([lines.join("\n")], { type: "text/x-python" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, "_")}_dag.py`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <Box sx={{ height: '100%' }}>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>Talend Job Converter</Typography>
          <Button color="inherit" component="label">
            Upload Folder
            <input type="file" webkitdirectory="true" multiple hidden onChange={handleFolderUpload} />
          </Button>
          {nodes.length > 0 && (
            <Button color="inherit" onClick={generateDag}>
              Download DAG
            </Button>
          )}
        </Toolbar>
      </AppBar>

      <Box sx={{ display: 'flex', height: 'calc(100% - 64px)' }}>
        <Box sx={{ width: 250, overflow: 'auto', borderRight: '1px solid #ccc' }}>
          <List>
            {jobName && (
              <ListItem button selected>
                <ListItemText primary={jobName} />
              </ListItem>
            )}
          </List>
        </Box>

        <Box sx={{ flexGrow: 1 }}>
          <ReactFlowProvider>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodeClick={onNodeClick}
              style={{ width: '100%', height: '100%' }}
            />
          </ReactFlowProvider>
        </Box>

        <Drawer anchor="right" open={Boolean(selectedNode)} onClose={() => setSelectedNode(null)}>
          {selectedNode && (
            <Box sx={{ width: 250, p: 2 }}>
              <Typography variant="h6" gutterBottom>{selectedNode.data.label}</Typography>
              <Typography variant="body2">ID: {selectedNode.id}</Typography>
            </Box>
          )}
        </Drawer>
      </Box>
    </Box>
  );
}

export default App;
