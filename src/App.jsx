import React, { useState } from 'react';
import {
  Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography,
  Box, Paper, CssBaseline, Dialog, DialogTitle, DialogContent, DialogActions
} from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import { ReactFlowProvider, ReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';

function App() {
  const [nodes, setNodes] = useState([]);
  const [realEdges, setRealEdges] = useState([]);
  const [jobName, setJobName] = useState('');
  const [selectedNode, setSelectedNode] = useState(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [comparisonResult, setComparisonResult] = useState('');

  const theme = createTheme({
    palette: { primary: { main: '#0288d1' }, background: { default: '#f5f7fa' } },
    typography: { fontFamily: 'Inter, Roboto, Arial, sans-serif' },
  });

  const handleCloseDialog = () => setDialogOpen(false);

  const handleFolderUpload = async (e) => {
    const files = Array.from(e.target.files);
    const itemFile = files.find(f => f.name.endsWith('.item'));
    if (!itemFile) return alert("No Talend .item file found.");

    const formData = new FormData();
    formData.append('file', itemFile);

    try {
      const res = await fetch('http://localhost:5000/upload', { method: 'POST', body: formData });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setNodes(data.nodes || []);
      setRealEdges(data.real_edges || []);
      setJobName(itemFile.name.replace('.item', ''));
    } catch (err) {
      console.error("Upload failed:", err);
      alert("Failed to parse job. Check console.");
    }
  };

  const onNodeClick = (event, node) => setSelectedNode(node);

  const generateDag = async () => {
    if (!jobName || nodes.length === 0) return;

    const activeNodes = nodes.filter(n => n.data.status === 'active');
    if (activeNodes.length === 0) {
      alert("No active components to generate DAG.");
      return;
    }

    const lines = [
      "from airflow import DAG",
      "from airflow.operators.python import PythonOperator",
      "from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook",
      "from hana_hook import HanaHook",
      "from datetime import datetime",
      "",
      "EXTRA_EMAILS = [\"XXXXXXXXXXXXXXXXXXXXXXXXXXX\"]",
      "def is_prod(): return False",
      "def is_develop(): return not is_prod()",
      "DEFAULT_ARGS = { 'owner': 'airflow', 'start_date': datetime(2023, 1, 1), 'email': [] }",
      "if is_prod(): DEFAULT_ARGS['email'].extend(EXTRA_EMAILS)",
      "SNOWFLAKE_CONNECTION = ('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' if is_develop() else 'YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')",
      "WAREHOUSE = 'XXXXXXXXXXXXXXXX' if is_prod() else 'YYYYYYYYYYYYYYYYYYY'",
      "DATABASE = 'XXXXXXXXXXXX' if is_prod() else 'YYYYYYYYYYYY'",
      "HANA_CONN = 'XXXXXXXXXXXXXXXXX'",
      "SCHEMA = 'YYYYYYYYY'",
      "DATABASE_SNOWFLAKE = 'XXXXXXXXXXXXX'",
      "",
      `with DAG(\"${jobName}\", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False) as dag:`
    ];

    const taskMap = {};
    let counter = {};
    const operatorLines = [];

    for (const node of activeNodes) {
      let base = node.id.replace(/[^a-zA-Z0-9_]/g, '_');
      counter[base] = (counter[base] || 0) + 1;
      const uniqueId = `${base}_${counter[base]}`;
      taskMap[node.id] = uniqueId;

      operatorLines.push(`    def task_${uniqueId}_fn():`);
      if ((node.data.label || '').toLowerCase().includes('snowflake')) {
        operatorLines.push(`        sf_hook = SnowflakeHook(snowflake_conn_id='SNOW_CONN_ID')`);
        operatorLines.push(`        sf_hook.run(\"\"\"${node.data.sql || ''}\"\"\")`);
      } else if ((node.data.label || '').toLowerCase().includes('hana')) {
        operatorLines.push(`        hana_hook = HanaHook(hana_conn_id='HANA_CONN_ID')`);
        operatorLines.push(`        hana_hook.get_records(\"\"\"${node.data.sql || ''}\"\"\")`);
      } else {
        operatorLines.push(`        print(\"${node.data.label || ''}\")`);
      }
      operatorLines.push(`\n    task_${uniqueId} = PythonOperator(`);
      operatorLines.push(`        task_id='${uniqueId}',`);
      operatorLines.push(`        python_callable=task_${uniqueId}_fn`);
      operatorLines.push(`    )\n`);
    }

    const edgeLines = [];
    realEdges.forEach(e => {
      const source = taskMap[e.source];
      const target = taskMap[e.target];
      if (source && target && source !== target) {
        edgeLines.push(`    task_${source} >> task_${target}`);
      }
    });

    const dagContent = [...lines, ...operatorLines, ...edgeLines].join('\n');
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
          job_steps: activeNodes.map(n => n.id),
          dag_text: dagContent,
        }),
      });
      if (res.ok) {
        const data = await res.json();
        setComparisonResult(data.result);
        setDialogOpen(true);
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
            <Button color="inherit" onClick={generateDag}>Download DAG</Button>
          </Toolbar>
        </AppBar>

        <Box sx={{ display: 'flex', height: 'calc(100% - 64px)', p: 2 }}>
          <Paper sx={{ width: 250, overflow: 'auto', mr: 2 }}>
            <List>{jobName && (<ListItem><ListItemText primary={jobName} /></ListItem>)}</List>
          </Paper>
          <Paper sx={{ flexGrow: 1, p: 1 }}>
            <ReactFlowProvider>
              <ReactFlow
                nodes={nodes.map(n => ({
                  ...n,
                  style: {
                    ...n.style,
                    background: n.data.status === 'active'
                      ? '#ffffff'
                      : n.data.status === 'deactivated'
                      ? '#ffebee'
                      : '#eeeeee',
                    border: '1px solid #bbb',
                    opacity: n.data.status === 'inactive' ? 0.6 : 1
                  }
                }))}
                edges={realEdges}
                onNodeClick={onNodeClick}
                style={{ width: '100%', height: '100%' }}
              />
            </ReactFlowProvider>
          </Paper>
          <Drawer anchor="right" open={!!selectedNode} onClose={() => setSelectedNode(null)}>
            {selectedNode && (
              <Box sx={{ width: 300, p: 2 }}>
                <Typography variant="h6">{selectedNode.data.label}</Typography>
                <Typography variant="body2">ID: {selectedNode.id}</Typography>
                <Typography variant="body2">Status: {selectedNode.data.status}</Typography>
                {selectedNode.data.sql && <Box component="pre" sx={{ mt: 2 }}>{selectedNode.data.sql}</Box>}
              </Box>
            )}
          </Drawer>
          <Dialog open={dialogOpen} onClose={handleCloseDialog}>
            <DialogTitle>Comparison Result</DialogTitle>
            <DialogContent><Box component="pre">{comparisonResult}</Box></DialogContent>
            <DialogActions><Button onClick={handleCloseDialog}>Close</Button></DialogActions>
          </Dialog>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
