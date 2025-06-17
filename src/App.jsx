import React, { useState } from 'react';
import {
  Button,
  List,
  ListItem,
  ListItemText,
  Drawer,
  AppBar,
  Toolbar,
  Typography,
  Box,
  Paper,
  CssBaseline,
} from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import { ReactFlowProvider, ReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';

function App() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [jobName, setJobName] = useState('');
  const [selectedNode, setSelectedNode] = useState(null);
  const theme = createTheme({
    palette: {
      primary: {
        main: '#0288d1',
      },
      background: {
        default: '#f5f7fa',
      },
    },
    typography: {
      fontFamily: 'Inter, Roboto, Arial, sans-serif',
    },
  });

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

  const generateDbtDag = async () => {
    if (!jobName) return;
    const lines = [];
    lines.push('# Example DBT DAG');
    lines.push('from airflow import DAG');
    lines.push('from airflow.operators.python import PythonOperator');
    lines.push('from datetime import datetime');
    lines.push('import jinja2');
    lines.push('');
    lines.push('DEFAULT_ARGS = {');
    lines.push("    'owner': 'airflow',");
    lines.push("    'start_date': datetime(2020, 7, 9),");
    lines.push('}');
    lines.push('');
    lines.push(`with DAG('${jobName}_dbt', default_args=DEFAULT_ARGS, catchup=False, schedule='05 21 * * *', template_undefined=jinja2.Undefined) as dag:`);
    lines.push('    delete_task = PythonOperator(');
    lines.push("        task_id='delete_less_records_from_hana',");
    lines.push('        python_callable=lambda: None');
    lines.push('    )');
    lines.push('    write_task = PythonOperator(');
    lines.push("        task_id='put_to_hana',");
    lines.push('        python_callable=lambda: None');
    lines.push('    )');
    lines.push('    delete_task >> write_task');

    const dagContent = lines.join('\n');
    const blob = new Blob([dagContent], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, '_')}_dbt_dag.py`;
    a.click();
    URL.revokeObjectURL(url);

    try {
      const res = await fetch('http://localhost:5000/compare_dag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          job_steps: nodes.map(n => n.id),
          dag_text: dagContent,
        }),
      });
      if (res.ok) {
        const data = await res.json();
        alert(data.result);
      } else {
        const errText = await res.text();
        console.error('Comparison failed:', errText);
      }
    } catch (err) {
      console.error('Comparison error:', err);
    }
  };

  const generateDag = async () => {
    if (!jobName || nodes.length === 0) return;

    const dagLines = [
      "from airflow import DAG",
      "from airflow.operators.python import PythonOperator",
      "from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook",
      "from hana_hook import HanaHook",
      "from datetime import datetime",
      "",
      "EXTRA_EMAILS = [",
      "    \"XXXXXXXXXXXXXXXXXXXXXXXXXXX\"",
      "]",
      "",
      "def is_prod():",
      "    return False",
      "",
      "def is_develop():",
      "    return not is_prod()",
      "",
      "DEFAULT_ARGS = {",
      "    'owner': 'airflow',",
      "    'start_date': datetime(2023, 1, 1),",
      "    'email': []",
      "}",
      "",
      "if is_prod():",
      "    DEFAULT_ARGS['email'].extend(EXTRA_EMAILS)",
      "",
      "SNOWFLAKE_CONNECTION = (",
      "    'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' if is_develop() else 'YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'",
      ")",
      "WAREHOUSE = 'XXXXXXXXXXXXXXXX' if is_prod() else 'YYYYYYYYYYYYYYYYYYY'",
      "DATABASE = 'XXXXXXXXXXXX' if is_prod() else 'YYYYYYYYYYYY'",
      "HANA_CONN = 'XXXXXXXXXXXXXXXXX'",
      "SCHEMA = 'YYYYYYYYY'",
      "DATABASE_SNOWFLAKE = 'XXXXXXXXXXXXX'",
      "",
      `with DAG("${jobName}", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False) as dag:`,
      "",
    ];

    const operatorLines = [];
    const taskIds = [];

    nodes.forEach((n) => {
      const compType = n.data.label?.toLowerCase() || "";
      if (compType.includes("connection")) return;

      const sql = n.data.sql || "";
      operatorLines.push(`    def task_${n.id}_fn():`);
      if (compType.includes("snowflake")) {
        operatorLines.push(`        sf_hook = SnowflakeHook(snowflake_conn_id='SNOW_CONN_ID')`);
        operatorLines.push(`        sf_hook.run("""${sql}""")`);
      } else if (compType.includes("hana")) {
        operatorLines.push(`        hana_hook = HanaHook(hana_conn_id='HANA_CONN_ID')`);
        operatorLines.push(`        hana_hook.get_records("""${sql}""")`);
      } else {
        operatorLines.push(`        print("${n.data.label}")`);
      }
      operatorLines.push("");
      operatorLines.push(`    task_${n.id} = PythonOperator(`);
      operatorLines.push(`        task_id='${n.id}',`);
      operatorLines.push(`        python_callable=task_${n.id}_fn`);
      operatorLines.push(`    )`);
      operatorLines.push("");

      taskIds.push(n.id);
    });

    const edgeLines = [];
    edges.forEach((e) => {
      if (taskIds.includes(e.source) && taskIds.includes(e.target)) {
        edgeLines.push(`    task_${e.source} >> task_${e.target}`);
      }
    });

    const lines = dagLines.concat(operatorLines, edgeLines);
    const dagContent = lines.join("\n");
    const blob = new Blob([dagContent], { type: "text/x-python" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, "_")}_dag.py`;
    a.click();
    URL.revokeObjectURL(url);

    try {
      const res = await fetch('http://localhost:5000/compare_dag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          job_steps: nodes.map(n => n.id),
          dag_text: dagContent,
        }),
      });
      if (res.ok) {
        const data = await res.json();
        alert(data.result);
      } else {
        const errText = await res.text();
        console.error('Comparison failed:', errText);
      }
    } catch (err) {
      console.error('Comparison error:', err);
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ height: '100%' }}>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" sx={{ flexGrow: 1 }}>Talend2AirDAG</Typography>
            <Button color="inherit" component="label">
              Upload Folder
              <input type="file" webkitdirectory="true" multiple hidden onChange={handleFolderUpload} />
            </Button>
            {nodes.length > 0 && (
              <Button color="inherit" onClick={generateDag}>
                Download DAG
              </Button>
            )}
            {nodes.length > 0 && (
              <Button color="inherit" onClick={generateDbtDag} sx={{ ml: 1 }}>
                Download DBT DAG
              </Button>
            )}
          </Toolbar>
        </AppBar>

        <Box sx={{ display: 'flex', height: 'calc(100% - 64px)', p: 2 }}>
          <Paper sx={{ width: 250, overflow: 'auto', mr: 2 }}>
            <List>
              {jobName && (
                <ListItem button selected>
                  <ListItemText primary={jobName} />
                </ListItem>
              )}
            </List>
          </Paper>

          <Paper sx={{ flexGrow: 1, p: 1 }}>
            <ReactFlowProvider>
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodeClick={onNodeClick}
                style={{ width: '100%', height: '100%' }}
              />
            </ReactFlowProvider>
          </Paper>

          <Drawer anchor="right" open={Boolean(selectedNode)} onClose={() => setSelectedNode(null)}>
            {selectedNode && (
              <Box sx={{ width: 300, p: 2 }}>
                <Typography variant="h6" gutterBottom>{selectedNode.data.label}</Typography>
                <Typography variant="body2" gutterBottom>ID: {selectedNode.id}</Typography>
                {selectedNode.data.sql && (
                  <Box component="pre" sx={{ whiteSpace: 'pre-wrap', fontSize: '0.75rem', mt: 1 }}>
                    {selectedNode.data.sql}
                  </Box>
                )}
                {selectedNode.data.procedure && (
                  <Typography variant="body2" sx={{ mt: 1 }}>Procedure: {selectedNode.data.procedure}</Typography>
                )}
              </Box>
            )}
          </Drawer>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
