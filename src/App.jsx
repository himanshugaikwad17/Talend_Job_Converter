import React, { useState, useCallback, useEffect } from 'react';
import {
  Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography,
  Box, Paper, CssBaseline, Dialog, DialogTitle, DialogContent, DialogActions
} from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import ReactFlow, {
  ReactFlowProvider,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType
} from 'reactflow';
import 'reactflow/dist/style.css';

function App() {
  const [allNodes, setAllNodes] = useState([]);
  const [allEdges, setAllEdges] = useState([]);
  const [jobName, setJobName] = useState('');
  const [selectedNode, setSelectedNode] = useState(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [comparisonResult, setComparisonResult] = useState('');

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const theme = createTheme({
    palette: { primary: { main: '#007acc' }, background: { default: '#f4f6f8' } },
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

      const alignedNodes = autoAlignGraph(data.nodes || [], data.edges || []);
      setAllNodes(alignedNodes);
      setAllEdges(data.edges || []);
      setNodes(alignedNodes);
      setEdges(data.edges || []);
      setJobName(itemFile.name.replace('.item', ''));
    } catch (err) {
      console.error("Upload failed:", err);
      alert("Failed to parse job. Check console.");
    }
  };

  const autoAlignGraph = (nodes, edges) => {
    const idToNode = Object.fromEntries(nodes.map(n => [n.id, n]));
    const inDegree = {};
    edges.forEach(e => { inDegree[e.target] = (inDegree[e.target] || 0) + 1 });

    const levels = [];
    const visited = new Set();
    const queue = nodes.filter(n => !inDegree[n.id]);

    let x = 50;
    while (queue.length) {
      const level = [];
      const nextQueue = [];

      queue.forEach((n, i) => {
        level.push({ ...n, position: { x, y: i * 180 + 100 } });
        visited.add(n.id);

        edges.filter(e => e.source === n.id).forEach(e => {
          if (!visited.has(e.target)) {
            inDegree[e.target]--;
            if (inDegree[e.target] === 0) nextQueue.push(idToNode[e.target]);
          }
        });
      });

      levels.push(...level);
      queue.splice(0, queue.length, ...nextQueue);
      x += 280;
    }

    return levels;
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
      "DEFAULT_ARGS = { 'owner': 'airflow', 'start_date': datetime(2023, 1, 1), 'email': [] }",
      `with DAG("${jobName}", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False) as dag:`
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
      operatorLines.push(`        print("Executing ${node.data.label}")`);
      operatorLines.push(`\n    task_${uniqueId} = PythonOperator(`);
      operatorLines.push(`        task_id='${uniqueId}',`);
      operatorLines.push(`        python_callable=task_${uniqueId}_fn`);
      operatorLines.push(`    )\n`);
    }

    const edgeLines = [];
    allEdges.forEach(e => {
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
          <Paper sx={{ width: 250, overflow: 'auto', mr: 2, p: 1 }} elevation={3}>
            <Typography variant="h6" sx={{ mb: 1 }}>Components</Typography>
            <List>
              {nodes.map((node) => (
                <ListItem key={node.id} button onClick={() => setSelectedNode(node)}>
                  <ListItemText primary={node.data.label} />
                </ListItem>
              ))}
            </List>
          </Paper>

          <Paper sx={{ flexGrow: 1, p: 1 }} elevation={3}>
            <ReactFlowProvider>
              <ReactFlow
                nodes={nodes.map(n => ({
                  ...n,
                  style: {
                    ...n.style,
                    background: n.data.status === 'active' ? '#ffffff'
                      : n.data.status === 'deactivated' ? '#ffebee' : '#eeeeee',
                    border: '1px solid #bbb',
                    borderRadius: '6px',
                    padding: 8,
                    fontSize: '12px',
                    opacity: n.data.status === 'inactive' ? 0.6 : 1
                  }
                }))}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeClick={onNodeClick}
                fitView
              >
                <MiniMap zoomable pannable />
                <Controls showInteractive={true} />
                <Background variant="dots" gap={12} size={1} />
              </ReactFlow>
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

          <Dialog open={dialogOpen} onClose={handleCloseDialog} maxWidth="md" fullWidth>
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
