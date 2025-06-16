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
      const res = await fetch('http://localhost:5000/upload?fake_edges=false', {
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
    lines.push('from airflow import DAG');
    lines.push('from airflow.operators.python import PythonOperator');
    lines.push('from datetime import datetime');
    lines.push('');
    lines.push(`with DAG("${jobName}", start_date=datetime(2023,1,1), schedule_interval=None) as dag:`);

    nodes.forEach(n => {
      lines.push(`    def task_${n.id}():`);
      lines.push(`        print("Running ${n.data.label}")`);
      lines.push('');
      lines.push(`    op_${n.id} = PythonOperator(task_id='${n.id}', python_callable=task_${n.id})`);
    });

    edges.forEach(e => {
      lines.push(`    op_${e.source} >> op_${e.target}`);
    });

    const blob = new Blob([lines.join('\n')], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, '_')}_dag.py`;
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
