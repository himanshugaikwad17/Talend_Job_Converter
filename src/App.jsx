import React, { useState, useCallback } from 'react';
import {
  Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography,
  Box, Paper, CssBaseline, Dialog, DialogTitle, DialogContent, DialogActions,
  Switch, FormControlLabel
} from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import ReactFlow, {
  ReactFlowProvider,
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState
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
      setAllNodes(data.nodes || []);
      setAllEdges(data.edges || []);
      setNodes(data.nodes || []);
      setEdges(data.edges || []);
      setJobName(itemFile.name.replace('.item', ''));
    } catch (err) {
      console.error("Upload failed:", err);
      alert("Failed to parse job. Check console.");
    }
  };

  const onNodeClick = (event, node) => setSelectedNode(node);

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
                    background: n.data.status === 'active' ? '#ffffff'
                      : n.data.status === 'deactivated' ? '#ffebee' : '#eeeeee',
                    border: '1px solid #bbb',
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
