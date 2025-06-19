import React, { useState } from 'react';
import {
  Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography,
  Box, Paper, CssBaseline, Dialog, DialogTitle, DialogContent, DialogActions,
  Snackbar, Alert, CircularProgress, IconButton, Tooltip
} from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import ReactFlow, {
  ReactFlowProvider,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
} from 'reactflow';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import DownloadIcon from '@mui/icons-material/Download';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import 'reactflow/dist/style.css';

function App() {
  const [allNodes, setAllNodes] = useState([]);
  const [allEdges, setAllEdges] = useState([]);
  const [jobName, setJobName] = useState('');
  const [selectedNode, setSelectedNode] = useState(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [comparisonResult, setComparisonResult] = useState('');
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'info' });
  const [loading, setLoading] = useState(false);
  const [aiDagDialogOpen, setAiDagDialogOpen] = useState(false);
  const [aiDagCode, setAiDagCode] = useState('');

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const theme = createTheme({
    palette: { primary: { main: '#007acc' }, background: { default: '#f4f6f8' } },
    typography: { fontFamily: 'Inter, Roboto, Arial, sans-serif' },
  });

  const handleCloseDialog = () => setDialogOpen(false);
  const handleCloseSnackbar = () => setSnackbar({ ...snackbar, open: false });

  const handleFolderUpload = async (e) => {
    const files = Array.from(e.target.files);
    const itemFile = files.find(f => f.name.endsWith('.item'));
    if (!itemFile) {
      setSnackbar({ open: true, message: "No Talend .item file found.", severity: 'warning' });
      return;
    }

    const formData = new FormData();
    formData.append('file', itemFile);

    setLoading(true);
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
      setSnackbar({ open: true, message: "Job loaded successfully!", severity: 'success' });
    } catch (err) {
      setSnackbar({ open: true, message: "Failed to parse job. Check console.", severity: 'error' });
      console.error("Upload failed:", err);
    }
    setLoading(false);
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
      setSnackbar({ open: true, message: "No active components to generate DAG.", severity: 'warning' });
      return;
    }

    setLoading(true);

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
    setLoading(false);
  };

  const handleGenerateDagAI = async () => {
    if (!jobName || nodes.length === 0) return;
    setLoading(true);
    setAiDagCode('');
    setAiDagDialogOpen(false);
    try {
      const job_structure = {
        nodes: allNodes,
        edges: allEdges,
        jobName,
      };
      const res = await fetch('http://localhost:5000/generate_dag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_structure }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      if (data.dag_code) {
        setAiDagCode(data.dag_code);
        setAiDagDialogOpen(true);
        setSnackbar({ open: true, message: 'AI-generated DAG ready!', severity: 'success' });
      } else {
        setSnackbar({ open: true, message: 'Failed to generate DAG with AI.', severity: 'error' });
      }
    } catch (err) {
      setSnackbar({ open: true, message: 'AI DAG generation failed. Check console.', severity: 'error' });
      console.error('AI DAG generation error:', err);
    }
    setLoading(false);
  };

  const handleDownloadAIDag = () => {
    const blob = new Blob([aiDagCode], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, '_')}_ai_dag.py`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', height: '100vh', bgcolor: 'background.default' }}>
        {/* Sidebar */}
        <Box sx={{
          width: 80, bgcolor: 'primary.main', color: '#fff', display: 'flex', flexDirection: 'column',
          alignItems: 'center', py: 2, boxShadow: 2
        }}>
          <Box sx={{ mb: 2 }}>
            <img src="https://cdn-icons-png.flaticon.com/512/5968/5968705.png" alt="Talend2AirDAG" width={40} />
          </Box>
          <Tooltip title="Info">
            <IconButton sx={{ color: '#fff' }}>
              <InfoOutlinedIcon />
            </IconButton>
          </Tooltip>
        </Box>

        {/* Main Content */}
        <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', height: '100vh' }}>
          {/* AppBar */}
          <AppBar position="static" color="inherit" elevation={1}>
            <Toolbar>
              <Typography variant="h6" sx={{ flexGrow: 1, color: 'primary.main', fontWeight: 700 }}>
                Talend2AirDAG
              </Typography>
              <Button
                variant="outlined"
                color="primary"
                component="label"
                startIcon={<CloudUploadIcon />}
                sx={{ mr: 2 }}
              >
                Upload Talend Job
                <input
                  type="file"
                  webkitdirectory="true"
                  multiple
                  hidden
                  onChange={handleFolderUpload}
                />
              </Button>
              <Button
                variant="contained"
                color="primary"
                startIcon={<DownloadIcon />}
                onClick={generateDag}
                disabled={nodes.length === 0 || loading}
                sx={{ mr: 2 }}
              >
                Download DAG
              </Button>
              <Button
                variant="contained"
                color="secondary"
                onClick={handleGenerateDagAI}
                disabled={nodes.length === 0 || loading}
              >
                Generate DAG with AI
              </Button>
            </Toolbar>
          </AppBar>

          {/* Job Info */}
          <Box sx={{
            px: 3, py: 2, bgcolor: '#fff', borderBottom: '1px solid #eee',
            display: 'flex', alignItems: 'center', minHeight: 60
          }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600, mr: 2 }}>
              {jobName ? `Job: ${jobName}` : 'No job loaded'}
            </Typography>
            {loading && <CircularProgress size={24} sx={{ ml: 2 }} />}
          </Box>

          {/* Main Layout */}
          <Box sx={{ display: 'flex', flexGrow: 1, overflow: 'hidden', p: 2 }}>
            {/* Components List */}
            <Paper sx={{ width: 260, overflow: 'auto', mr: 2, p: 2, borderRadius: 3, boxShadow: 2 }}>
              <Typography variant="h6" sx={{ mb: 1, color: 'primary.main' }}>Components</Typography>
              <List>
                {nodes.map((node) => (
                  <ListItem
                    key={node.id}
                    button
                    onClick={() => setSelectedNode(node)}
                    sx={{
                      bgcolor: selectedNode?.id === node.id ? 'primary.light' : 'inherit',
                      borderRadius: 2,
                      mb: 1,
                      '&:hover': { bgcolor: 'primary.lighter' }
                    }}
                  >
                    <ListItemText primary={node.data.label} />
                  </ListItem>
                ))}
              </List>
            </Paper>

            {/* Graph */}
            <Paper sx={{ flexGrow: 1, p: 1, borderRadius: 3, boxShadow: 2, minWidth: 0 }}>
              <ReactFlowProvider>
                <ReactFlow
                  nodes={nodes.map(n => ({
                    ...n,
                    style: {
                      ...n.style,
                      background: n.data.status === 'active' ? '#ffffff'
                        : n.data.status === 'deactivated' ? '#ffebee' : '#eeeeee',
                      border: '1.5px solid #007acc',
                      borderRadius: '8px',
                      padding: 8,
                      fontSize: '13px',
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

            {/* Node Drawer */}
            <Drawer anchor="right" open={!!selectedNode} onClose={() => setSelectedNode(null)}>
              {selectedNode && (
                <Box sx={{ width: 320, p: 3 }}>
                  <Typography variant="h6" sx={{ color: 'primary.main' }}>{selectedNode.data.label}</Typography>
                  <Typography variant="body2" sx={{ mt: 1 }}>ID: {selectedNode.id}</Typography>
                  <Typography variant="body2">Status: {selectedNode.data.status}</Typography>
                  {selectedNode.data.sql && (
                    <Box component="pre" sx={{
                      mt: 2, bgcolor: '#f5f5f5', p: 2, borderRadius: 2, fontSize: 13, overflowX: 'auto'
                    }}>
                      {selectedNode.data.sql}
                    </Box>
                  )}
                </Box>
              )}
            </Drawer>

            {/* Comparison Dialog */}
            <Dialog open={dialogOpen} onClose={handleCloseDialog} maxWidth="md" fullWidth>
              <DialogTitle>Comparison Result</DialogTitle>
              <DialogContent>
                <Box component="pre" sx={{ whiteSpace: 'pre-wrap', fontSize: 14 }}>
                  {comparisonResult}
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={handleCloseDialog}>Close</Button>
              </DialogActions>
            </Dialog>

            {/* AI DAG Dialog */}
            <Dialog open={aiDagDialogOpen} onClose={() => setAiDagDialogOpen(false)} maxWidth="md" fullWidth>
              <DialogTitle>AI-Generated Airflow DAG</DialogTitle>
              <DialogContent>
                <Box component="pre" sx={{ whiteSpace: 'pre-wrap', fontSize: 14, maxHeight: 500, overflowY: 'auto' }}>
                  {aiDagCode}
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={handleDownloadAIDag} disabled={!aiDagCode}>Download</Button>
                <Button onClick={() => setAiDagDialogOpen(false)}>Close</Button>
              </DialogActions>
            </Dialog>
          </Box>
        </Box>

        {/* Snackbar for feedback */}
        <Snackbar
          open={snackbar.open}
          autoHideDuration={4000}
          onClose={handleCloseSnackbar}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        >
          <Alert onClose={handleCloseSnackbar} severity={snackbar.severity} sx={{ width: '100%' }}>
            {snackbar.message}
          </Alert>
        </Snackbar>
      </Box>
    </ThemeProvider>
  );
}

export default App;
