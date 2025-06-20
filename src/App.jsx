import React, { useState } from 'react';
import {
  Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography,
  Box, Paper, CssBaseline, Dialog, DialogTitle, DialogContent, DialogActions,
  Snackbar, Alert, CircularProgress, IconButton, Tooltip, Divider, Chip,
  ListItemIcon, Accordion, AccordionSummary, AccordionDetails, Table,
  TableBody, TableCell, TableContainer, TableHead, TableRow, Grid
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
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { materialDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import DownloadIcon from '@mui/icons-material/Download';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import CodeIcon from '@mui/icons-material/Code';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import PauseCircleIcon from '@mui/icons-material/PauseCircle';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';
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
  const [lineageDialogOpen, setLineageDialogOpen] = useState(false);
  const [lineageData, setLineageData] = useState(null);
  const [lineageLoading, setLineageLoading] = useState(false);
  const [sqlDialogOpen, setSqlDialogOpen] = useState(false);
  const [selectedSql, setSelectedSql] = useState(null);
  const [previewDialogOpen, setPreviewDialogOpen] = useState(false);
  const [previewDagCode, setPreviewDagCode] = useState('');
  const [showLineage, setShowLineage] = useState(false);
  const [manualSqlDialogOpen, setManualSqlDialogOpen] = useState(false);
  const [currentComponentIndex, setCurrentComponentIndex] = useState(0);
  const [manualDagCode, setManualDagCode] = useState('');

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

  const handleDownloadUpdatedDag = () => {
    const blob = new Blob([aiDagCode], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${jobName.replace(/\s+/g, '_')}_updated_dag.py`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleLineageComparison = async () => {
    if (!jobName || nodes.length === 0 || !aiDagCode) {
      setSnackbar({ open: true, message: 'Please generate a DAG first', severity: 'warning' });
      return;
    }
    
    setLineageLoading(true);
    try {
      const job_structure = {
        nodes: allNodes,
        edges: allEdges,
        jobName,
      };
      
      const res = await fetch('http://localhost:5000/generate_lineage_comparison', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          job_structure,
          dag_code: aiDagCode 
        }),
      });
      
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      
      // Forcefully replace the SQL in the mapping with the original, full SQL from allNodes
      if (data.lineage_mapping) {
          data.lineage_mapping.forEach(mapping => {
              if (mapping.talend_component) {
                  const originalNode = allNodes.find(n => n.id === mapping.talend_component.id);
                  if (originalNode && originalNode.data.sql) {
                      mapping.talend_component.sql = originalNode.data.sql;
                  }
              }
          });
      }
      
      setLineageData(data);
      setLineageDialogOpen(true);
      setSnackbar({ open: true, message: 'Lineage comparison ready!', severity: 'success' });
    } catch (err) {
      setSnackbar({ open: true, message: 'Lineage comparison failed. Check console.', severity: 'error' });
      console.error('Lineage comparison error:', err);
    }
    setLineageLoading(false);
  };

  const handleManualSqlInjection = () => {
    if (!lineageData || !lineageData.lineage_mapping || !aiDagCode) {
      setSnackbar({ open: true, message: 'Please run lineage comparison first', severity: 'warning' });
      return;
    }
    
    // Filter components that have SQL
    const componentsWithSql = lineageData.lineage_mapping.filter(
      mapping => mapping.talend_component && mapping.talend_component.sql
    );
    
    if (componentsWithSql.length === 0) {
      setSnackbar({ open: true, message: 'No components with SQL found', severity: 'info' });
      return;
    }
    
    setManualDagCode(aiDagCode);
    setCurrentComponentIndex(0);
    setManualSqlDialogOpen(true);
    setLineageDialogOpen(false);
  };

  const handleInjectSqlForComponent = (componentIndex) => {
    if (!lineageData || !lineageData.lineage_mapping) return;
    
    const componentsWithSql = lineageData.lineage_mapping.filter(
      mapping => mapping.talend_component && mapping.talend_component.sql
    );
    
    if (componentIndex >= componentsWithSql.length) return;
    
    const mapping = componentsWithSql[componentIndex];
    const talendComponent = mapping.talend_component;
    const airflowTask = mapping.airflow_task;
    
    if (!airflowTask) {
      setSnackbar({ open: true, message: `No Airflow task found for ${talendComponent.name}`, severity: 'warning' });
      return;
    }
    
    // Find and replace the SQL in the DAG code
    let updatedDagCode = manualDagCode;
    const taskPattern = new RegExp(`(${airflowTask.variable_name}\\s*=\\s*SnowflakeOperator\\s*\\([^)]*sql\\s*=\\s*['"\`]?)[^'"\`]*['"\`]?`, 'g');
    
    if (taskPattern.test(updatedDagCode)) {
      updatedDagCode = updatedDagCode.replace(taskPattern, `$1'''${talendComponent.sql}'''`);
      setManualDagCode(updatedDagCode);
      setSnackbar({ open: true, message: `SQL injected for ${talendComponent.name}`, severity: 'success' });
    } else {
      setSnackbar({ open: true, message: `Could not find task ${airflowTask.task_id} in DAG`, severity: 'error' });
    }
  };

  const handleNextComponent = () => {
    const componentsWithSql = lineageData.lineage_mapping.filter(
      mapping => mapping.talend_component && mapping.talend_component.sql
    );
    
    if (currentComponentIndex < componentsWithSql.length - 1) {
      setCurrentComponentIndex(currentComponentIndex + 1);
    }
  };

  const handlePreviousComponent = () => {
    if (currentComponentIndex > 0) {
      setCurrentComponentIndex(currentComponentIndex - 1);
    }
  };

  const handleFinishManualInjection = () => {
    setAiDagCode(manualDagCode);
    setPreviewDagCode(manualDagCode);
    setPreviewDialogOpen(true);
    setManualSqlDialogOpen(false);
    setSnackbar({ open: true, message: 'Manual SQL injection completed! Preview the updated DAG.', severity: 'success' });
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
                color="secondary"
                onClick={handleGenerateDagAI}
                disabled={nodes.length === 0 || loading}
              >
                Generate DAG with AI
              </Button>
              <Button
                variant="outlined"
                color="primary"
                onClick={handleLineageComparison}
                disabled={nodes.length === 0 || !aiDagCode || lineageLoading}
                startIcon={<CompareArrowsIcon />}
                sx={{ ml: 2 }}
              >
                {lineageLoading ? 'Analyzing...' : 'Compare Lineage'}
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
            <Paper sx={{ width: 280, overflow: 'auto', mr: 2, p: 2, borderRadius: 3, boxShadow: 2 }}>
              <Typography variant="h6" sx={{ mb: 2, color: 'primary.main', fontWeight: 600 }}>
                Job Components
              </Typography>
              
              {/* Summary Stats */}
              <Box sx={{ mb: 2, p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
                <Typography variant="body2" sx={{ color: 'text.secondary', mb: 1 }}>
                  Component Status Summary
                </Typography>
                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                  <Chip 
                    icon={<CheckCircleIcon />}
                    label={`${nodes.filter(n => n.data.status === 'active').length} Active`}
                    size="small"
                    color="success"
                    variant="outlined"
                  />
                  <Chip 
                    icon={<PauseCircleIcon />}
                    label={`${nodes.filter(n => n.data.status === 'inactive').length} Inactive`}
                    size="small"
                    color="warning"
                    variant="outlined"
                  />
                  <Chip 
                    icon={<CancelIcon />}
                    label={`${nodes.filter(n => n.data.status === 'deactivated').length} Deactivated`}
                    size="small"
                    color="error"
                    variant="outlined"
                  />
                </Box>
              </Box>

              {/* Active Components */}
              <Accordion defaultExpanded sx={{ mb: 1, '&:before': { display: 'none' } }}>
                <AccordionSummary 
                  expandIcon={<ExpandMoreIcon />}
                  sx={{ 
                    bgcolor: 'success.light', 
                    color: 'success.contrastText',
                    borderRadius: 1,
                    '&:hover': { bgcolor: 'success.main' }
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                    <CheckCircleIcon sx={{ mr: 1 }} />
                    <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                      Active Components ({nodes.filter(n => n.data.status === 'active').length})
                    </Typography>
                  </Box>
                </AccordionSummary>
                <AccordionDetails sx={{ p: 0 }}>
                  <List dense>
                    {nodes
                      .filter(node => node.data.status === 'active')
                      .map((node) => (
                        <ListItem
                          key={node.id}
                          button
                          onClick={() => setSelectedNode(node)}
                          sx={{
                            bgcolor: selectedNode?.id === node.id ? 'success.light' : 'inherit',
                            borderRadius: 1,
                            mb: 0.5,
                            '&:hover': { bgcolor: 'success.lighter' }
                          }}
                        >
                          <ListItemIcon sx={{ minWidth: 32 }}>
                            <CheckCircleIcon color="success" fontSize="small" />
                          </ListItemIcon>
                          <ListItemText 
                            primary={node.data.label}
                            secondary={node.data.component_type}
                            primaryTypographyProps={{ fontSize: '0.875rem', fontWeight: 500 }}
                            secondaryTypographyProps={{ fontSize: '0.75rem' }}
                          />
                        </ListItem>
                      ))}
                  </List>
                </AccordionDetails>
              </Accordion>

              {/* Inactive Components */}
              {nodes.filter(n => n.data.status === 'inactive').length > 0 && (
                <Accordion sx={{ mb: 1, '&:before': { display: 'none' } }}>
                  <AccordionSummary 
                    expandIcon={<ExpandMoreIcon />}
                    sx={{ 
                      bgcolor: 'warning.light', 
                      color: 'warning.contrastText',
                      borderRadius: 1,
                      '&:hover': { bgcolor: 'warning.main' }
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                      <PauseCircleIcon sx={{ mr: 1 }} />
                      <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                        Inactive Components ({nodes.filter(n => n.data.status === 'inactive').length})
                      </Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails sx={{ p: 0 }}>
                    <List dense>
                      {nodes
                        .filter(node => node.data.status === 'inactive')
                        .map((node) => (
                          <ListItem
                            key={node.id}
                            button
                            onClick={() => setSelectedNode(node)}
                            sx={{
                              bgcolor: selectedNode?.id === node.id ? 'warning.light' : 'inherit',
                              borderRadius: 1,
                              mb: 0.5,
                              '&:hover': { bgcolor: 'warning.lighter' }
                            }}
                          >
                            <ListItemIcon sx={{ minWidth: 32 }}>
                              <PauseCircleIcon color="warning" fontSize="small" />
                            </ListItemIcon>
                            <ListItemText 
                              primary={node.data.label}
                              secondary={node.data.component_type}
                              primaryTypographyProps={{ fontSize: '0.875rem', fontWeight: 500 }}
                              secondaryTypographyProps={{ fontSize: '0.75rem' }}
                            />
                          </ListItem>
                        ))}
                    </List>
                  </AccordionDetails>
                </Accordion>
              )}

              {/* Deactivated Components */}
              {nodes.filter(n => n.data.status === 'deactivated').length > 0 && (
                <Accordion sx={{ mb: 1, '&:before': { display: 'none' } }}>
                  <AccordionSummary 
                    expandIcon={<ExpandMoreIcon />}
                    sx={{ 
                      bgcolor: 'error.light', 
                      color: 'error.contrastText',
                      borderRadius: 1,
                      '&:hover': { bgcolor: 'error.main' }
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                      <CancelIcon sx={{ mr: 1 }} />
                      <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                        Deactivated Components ({nodes.filter(n => n.data.status === 'deactivated').length})
                      </Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails sx={{ p: 0 }}>
                    <List dense>
                      {nodes
                        .filter(node => node.data.status === 'deactivated')
                        .map((node) => (
                          <ListItem
                            key={node.id}
                            button
                            onClick={() => setSelectedNode(node)}
                            sx={{
                              bgcolor: selectedNode?.id === node.id ? 'error.light' : 'inherit',
                              borderRadius: 1,
                              mb: 0.5,
                              '&:hover': { bgcolor: 'error.lighter' }
                            }}
                          >
                            <ListItemIcon sx={{ minWidth: 32 }}>
                              <CancelIcon color="error" fontSize="small" />
                            </ListItemIcon>
                            <ListItemText 
                              primary={node.data.label}
                              secondary={node.data.component_type}
                              primaryTypographyProps={{ fontSize: '0.875rem', fontWeight: 500 }}
                              secondaryTypographyProps={{ fontSize: '0.75rem' }}
                            />
                          </ListItem>
                        ))}
                    </List>
                  </AccordionDetails>
                </Accordion>
              )}

              {/* No Components Message */}
              {nodes.length === 0 && (
                <Box sx={{ textAlign: 'center', py: 3, color: 'text.secondary' }}>
                  <Typography variant="body2">
                    No components loaded
                  </Typography>
                  <Typography variant="caption">
                    Upload a Talend job to see components
                  </Typography>
                </Box>
              )}
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
            <Drawer 
              anchor="right" 
              open={!!selectedNode} 
              onClose={() => setSelectedNode(null)}
              PaperProps={{ sx: { width: 480 } }}
            >
              {selectedNode && (
                <Box sx={{ p: 3 }}>
                  <Typography variant="h6" sx={{ color: 'primary.main', mb: 2, display: 'flex', alignItems: 'center' }}>
                    <CodeIcon sx={{ mr: 1 }} />
                    Component Details
                  </Typography>
                  
                  {/* Basic Info */}
                  <Paper sx={{ p: 2, mb: 2, borderRadius: 2 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      {selectedNode.data.label}
                    </Typography>
                    <Box sx={{ mt: 1 }}>
                      <Chip 
                        label={selectedNode.data.status} 
                        size="small"
                        color={
                          selectedNode.data.status === 'active' ? 'success' :
                          selectedNode.data.status === 'deactivated' ? 'error' : 'default'
                        }
                        sx={{ mr: 1 }}
                      />
                      <Chip 
                        label={selectedNode.id} 
                        size="small" 
                        variant="outlined"
                      />
                    </Box>
                  </Paper>
                  
                  {/* SQL Content */}
                  {selectedNode.data.sql && (
                    <Paper sx={{ mt: 3, borderRadius: 2, overflow: 'hidden' }}>
                      <Box sx={{ bgcolor: 'grey.900', px: 2, py: 1 }}>
                        <Typography variant="subtitle2" sx={{ color: 'white' }}>
                          SQL Query
                        </Typography>
                      </Box>
                      <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                        <SyntaxHighlighter 
                          language="sql"
                          style={materialDark}
                          customStyle={{ margin: 0, borderRadius: 0 }}
                        >
                          {selectedNode.data.sql}
                        </SyntaxHighlighter>
                      </Box>
                    </Paper>
                  )}
                  
                  {/* Component Type Info */}
                  <Paper sx={{ p: 2, mt: 2, borderRadius: 2 }}>
                    <Typography variant="subtitle2" sx={{ color: 'text.secondary', mb: 1 }}>
                      Component Type
                    </Typography>
                    <Typography variant="body2">
                      {selectedNode.data.component_type || 'Unknown'}
                    </Typography>
                  </Paper>
                  
                  {/* Connections */}
                  <Paper sx={{ p: 2, mt: 2, borderRadius: 2 }}>
                    <Typography variant="subtitle2" sx={{ color: 'text.secondary', mb: 1 }}>
                      Connections
                    </Typography>
                    <Box>
                      {edges
                        .filter(e => e.source === selectedNode.id || e.target === selectedNode.id)
                        .map((edge, idx) => (
                          <Typography key={idx} variant="body2" sx={{ mb: 0.5 }}>
                            {edge.source === selectedNode.id ? 'Output → ' : '← Input: '}
                            {nodes.find(n => 
                              n.id === (edge.source === selectedNode.id ? edge.target : edge.source)
                            )?.data.label || 'Unknown'}
                          </Typography>
                        ))}
                    </Box>
                  </Paper>
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
                <Button onClick={handleDownloadUpdatedDag} disabled={!aiDagCode}>Download Updated</Button>
                <Button onClick={() => setAiDagDialogOpen(false)}>Close</Button>
              </DialogActions>
            </Dialog>
            
            {/* Lineage Comparison Dialog */}
            <Dialog open={lineageDialogOpen} onClose={() => setLineageDialogOpen(false)} maxWidth="lg" fullWidth>
              <DialogTitle>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <CompareArrowsIcon sx={{ mr: 1 }} />
                  Lineage Comparison: Talend → Airflow
                </Box>
              </DialogTitle>
              <DialogContent>
                {lineageData && (
                  <Box sx={{ mt: 2 }}>
                    {/* Summary Stats */}
                    <Paper sx={{ p: 2, mb: 3, bgcolor: 'grey.50' }}>
                      <Typography variant="h6" sx={{ mb: 2, color: 'primary.main' }}>
                        Migration Summary
                      </Typography>
                      <Grid container spacing={2}>
                        <Grid item xs={3}>
                          <Box sx={{ textAlign: 'center' }}>
                            <Typography variant="h4" color="primary">
                              {lineageData.comparison_summary.talend_components}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Talend Components
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={3}>
                          <Box sx={{ textAlign: 'center' }}>
                            <Typography variant="h4" color="secondary">
                              {lineageData.comparison_summary.airflow_tasks}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Airflow Tasks
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={3}>
                          <Box sx={{ textAlign: 'center' }}>
                            <Typography variant="h4" color="success.main">
                              {lineageData.comparison_summary.mapping_percentage}%
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Mapping Success
                            </Typography>
                          </Box>
                        </Grid>
                        <Grid item xs={3}>
                          <Box sx={{ textAlign: 'center' }}>
                            <Typography variant="h4" color="warning.main">
                              {lineageData.comparison_summary.unmapped_components}
                            </Typography>
                            <Typography variant="body2" color="text.secondary">
                              Unmapped
                            </Typography>
                          </Box>
                        </Grid>
                      </Grid>
                    </Paper>

                    {/* Detailed Mapping Table */}
                    <Paper sx={{ mb: 3 }}>
                      <Box sx={{ p: 2, bgcolor: 'primary.main', color: 'white' }}>
                        <Typography variant="h6">
                          Component Mapping Details
                        </Typography>
                      </Box>
                      <TableContainer>
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableCell sx={{ fontWeight: 600 }}>Talend Component</TableCell>
                              <TableCell sx={{ fontWeight: 600 }}>Type</TableCell>
                              <TableCell sx={{ fontWeight: 600 }}>Airflow Task</TableCell>
                              <TableCell sx={{ fontWeight: 600 }}>Operator</TableCell>
                              <TableCell sx={{ fontWeight: 600 }}>Status</TableCell>
                              <TableCell sx={{ fontWeight: 600 }}>Confidence</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {lineageData.lineage_mapping.map((mapping, index) => (
                              <TableRow key={index}>
                                <TableCell>
                                  <Box>
                                    <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                      {mapping.talend_component.name}
                                    </Typography>
                                    <Typography variant="caption" color="text.secondary">
                                      {mapping.talend_component.id}
                                    </Typography>
                                  </Box>
                                </TableCell>
                                <TableCell>
                                  <Chip 
                                    label={mapping.talend_component.type} 
                                    size="small" 
                                    variant="outlined"
                                  />
                                </TableCell>
                                <TableCell>
                                  {mapping.airflow_task ? (
                                    <Box>
                                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                        {mapping.airflow_task.task_id}
                                      </Typography>
                                      <Typography variant="caption" color="text.secondary">
                                        {mapping.airflow_task.variable_name}
                                      </Typography>
                                      {/* Add SQL preview button if component has SQL */}
                                      {mapping.talend_component.sql && (
                                        <IconButton
                                          size="small"
                                          onClick={() => {
                                            setSelectedSql({
                                              talendSql: mapping.talend_component.sql,
                                              componentName: mapping.talend_component.name,
                                              taskId: mapping.airflow_task.task_id
                                            });
                                            setSqlDialogOpen(true);
                                          }}
                                          sx={{ ml: 1 }}
                                        >
                                          <CodeIcon fontSize="small" />
                                        </IconButton>
                                      )}
                                    </Box>
                                  ) : (
                                    <Typography variant="body2" color="text.secondary">
                                      Not mapped
                                    </Typography>
                                  )}
                                </TableCell>
                                <TableCell>
                                  {mapping.airflow_task ? (
                                    <Chip 
                                      label={mapping.airflow_task.operator_type} 
                                      size="small" 
                                      color="primary"
                                    />
                                  ) : (
                                    <Typography variant="body2" color="text.secondary">
                                      -
                                    </Typography>
                                  )}
                                </TableCell>
                                <TableCell>
                                  <Chip 
                                    label={mapping.mapping_type === 'direct' ? 'Direct Match' :
                                          mapping.mapping_type === 'connection' ? 'Connection Mapped' :
                                          mapping.mapping_type === 'sql_execution' ? 'SQL Execution' :
                                          'Unmapped'}
                                    size="small"
                                    color={mapping.mapping_type === 'unmapped' ? 'warning' : 'success'}
                                    icon={mapping.mapping_type === 'unmapped' ? <CancelIcon /> : <CheckCircleIcon />}
                                  />
                                  {mapping.notes && (
                                    <Typography variant="caption" display="block" sx={{ mt: 0.5, color: 'text.secondary' }}>
                                      {mapping.notes}
                                    </Typography>
                                  )}
                                </TableCell>

                                {/* Add confidence indicator */}
                                <TableCell>
                                  <Chip 
                                    label={mapping.confidence}
                                    size="small"
                                    color={
                                      mapping.confidence === 'high' ? 'success' :
                                      mapping.confidence === 'medium' ? 'primary' :
                                      'warning'
                                    }
                                    variant="outlined"
                                  />
                                </TableCell>
                              </TableRow>
                            ))}
                            
                            {/* Add mapping type summary */}
                            <TableRow sx={{ bgcolor: 'grey.50' }}>
                              <TableCell colSpan={6}>
                                <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', py: 1 }}>
                                  <Typography variant="subtitle2" sx={{ color: 'text.secondary' }}>
                                    Mapping Types:
                                  </Typography>
                                  <Chip 
                                    label={`${lineageData.comparison_summary.mapping_details.direct_mappings} Direct`}
                                    size="small"
                                    color="success"
                                    variant="outlined"
                                  />
                                  <Chip 
                                    label={`${lineageData.comparison_summary.mapping_details.connection_mappings} Connection`}
                                    size="small"
                                    color="primary"
                                    variant="outlined"
                                  />
                                  <Chip 
                                    label={`${lineageData.comparison_summary.mapping_details.sql_mappings} SQL`}
                                    size="small"
                                    color="secondary"
                                    variant="outlined"
                                  />
                                </Box>
                              </TableCell>
                            </TableRow>
                          </TableBody>
                        </Table>
                      </TableContainer>
                    </Paper>

                    {/* Flow Comparison */}
                    <Paper>
                      <Box sx={{ p: 2, bgcolor: 'secondary.main', color: 'white' }}>
                        <Typography variant="h6">
                          Data Flow Comparison
                        </Typography>
                      </Box>
                      <Box sx={{ p: 2 }}>
                        <Grid container spacing={3}>
                          <Grid item xs={6}>
                            <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 600 }}>
                              Talend Flow Connections
                            </Typography>
                            <List dense>
                              {lineageData.talend_flow.connections.map((conn, index) => (
                                <ListItem key={index} sx={{ py: 0.5 }}>
                                  <ListItemIcon sx={{ minWidth: 32 }}>
                                    <Box sx={{ width: 8, height: 8, bgcolor: 'primary.main', borderRadius: '50%' }} />
                                  </ListItemIcon>
                                  <ListItemText 
                                    primary={`${conn.from} → ${conn.to}`}
                                    primaryTypographyProps={{ fontSize: '0.875rem' }}
                                  />
                                </ListItem>
                              ))}
                            </List>
                          </Grid>
                          <Grid item xs={6}>
                            <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 600 }}>
                              Airflow Dependencies
                            </Typography>
                            <List dense>
                              {lineageData.airflow_dag.map((task, index) => (
                                <ListItem key={index} sx={{ py: 0.5 }}>
                                  <ListItemIcon sx={{ minWidth: 32 }}>
                                    <Box sx={{ width: 8, height: 8, bgcolor: 'secondary.main', borderRadius: '50%' }} />
                                  </ListItemIcon>
                                  <ListItemText 
                                    primary={`${task.task_id} → ${task.dependencies.join(', ') || 'None'}`}
                                    primaryTypographyProps={{ fontSize: '0.875rem' }}
                                  />
                                </ListItem>
                              ))}
                            </List>
                          </Grid>
                        </Grid>
                      </Box>
                    </Paper>
                  </Box>
                )}
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setLineageDialogOpen(false)}>Close</Button>
                <Button 
                  variant="contained" 
                  color="primary"
                  onClick={handleManualSqlInjection}
                  disabled={!lineageData || lineageLoading}
                >
                  Manual SQL Injection
                </Button>
                <Button 
                  variant="outlined"
                  color="secondary"
                  onClick={handleDownloadUpdatedDag}
                  disabled={!aiDagCode}
                  startIcon={<DownloadIcon />}
                >
                  Download Updated DAG
                </Button>
              </DialogActions>
            </Dialog>

            {/* SQL Preview Dialog */}
            <Dialog 
              open={sqlDialogOpen} 
              onClose={() => setSqlDialogOpen(false)} 
              maxWidth="md" 
              fullWidth
            >
              <DialogTitle>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <CodeIcon sx={{ mr: 1 }} />
                  SQL Mapping Details
                </Box>
              </DialogTitle>
              <DialogContent>
                {selectedSql && (
                  <Box sx={{ mt: 2 }}>
                    {/* Component Info */}
                    <Paper sx={{ p: 2, mb: 3, bgcolor: 'primary.light', color: 'primary.contrastText' }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                        {selectedSql.componentName}
                      </Typography>
                      <Typography variant="body2">
                        Mapped to Airflow task: {selectedSql.taskId}
                      </Typography>
                    </Paper>

                    {/* SQL Code */}
                    <Typography variant="subtitle2" sx={{ mb: 1, color: 'text.secondary' }}>
                      SQL Statement
                    </Typography>
                    <Paper sx={{ mb: 3 }}>
                      <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                        <SyntaxHighlighter 
                          language="sql"
                          style={materialDark}
                          customStyle={{ margin: 0, borderRadius: 4 }}
                        >
                          {selectedSql.talendSql}
                        </SyntaxHighlighter>
                      </Box>
                    </Paper>

                    {/* Usage Info */}
                    <Paper sx={{ p: 2, bgcolor: 'success.light', color: 'success.contrastText' }}>
                      <Typography variant="subtitle2" sx={{ mb: 1 }}>
                        ✓ SQL Successfully Mapped
                      </Typography>
                      <Typography variant="body2">
                        This SQL has been automatically included in the corresponding Airflow task.
                      </Typography>
                    </Paper>
                  </Box>
                )}
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setSqlDialogOpen(false)}>Close</Button>
              </DialogActions>
            </Dialog>

            {/* Manual SQL Injection Dialog */}
            <Dialog 
              open={manualSqlDialogOpen} 
              onClose={() => setManualSqlDialogOpen(false)} 
              maxWidth="lg" 
              fullWidth
            >
              <DialogTitle>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <CodeIcon sx={{ mr: 1 }} />
                    Manual SQL Injection
                  </Box>
                  {lineageData && (
                    <Chip 
                      label={`${currentComponentIndex + 1} of ${lineageData.lineage_mapping.filter(m => m.talend_component?.sql).length}`}
                      color="primary"
                      variant="outlined"
                    />
                  )}
                </Box>
              </DialogTitle>
              <DialogContent>
                {lineageData && lineageData.lineage_mapping && (() => {
                  const componentsWithSql = lineageData.lineage_mapping.filter(
                    mapping => mapping.talend_component && mapping.talend_component.sql
                  );
                  
                  if (currentComponentIndex >= componentsWithSql.length) return null;
                  
                  const mapping = componentsWithSql[currentComponentIndex];
                  const talendComponent = mapping.talend_component;
                  const airflowTask = mapping.airflow_task;
                  
                  return (
                    <Box sx={{ mt: 2 }}>
                      {/* Component Info */}
                      <Paper sx={{ p: 2, mb: 3, bgcolor: 'primary.light', color: 'primary.contrastText' }}>
                        <Typography variant="h6" sx={{ fontWeight: 600 }}>
                          {talendComponent.name}
                        </Typography>
                        <Typography variant="body2">
                          Type: {talendComponent.type}
                        </Typography>
                        {airflowTask && (
                          <Typography variant="body2">
                            Airflow Task: {airflowTask.task_id}
                          </Typography>
                        )}
                      </Paper>

                      {/* SQL Code */}
                      <Typography variant="subtitle2" sx={{ mb: 1, color: 'text.secondary' }}>
                        SQL Statement to Inject
                      </Typography>
                      <Paper sx={{ mb: 3 }}>
                        <Box sx={{ maxHeight: 300, overflow: 'auto' }}>
                          <SyntaxHighlighter 
                            language="sql"
                            style={materialDark}
                            customStyle={{ margin: 0, borderRadius: 4 }}
                          >
                            {talendComponent.sql}
                          </SyntaxHighlighter>
                        </Box>
                      </Paper>

                      {/* Action Buttons */}
                      <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
                        <Button 
                          variant="contained" 
                          color="primary"
                          onClick={() => handleInjectSqlForComponent(currentComponentIndex)}
                          disabled={!airflowTask}
                        >
                          Inject SQL into Task
                        </Button>
                        {!airflowTask && (
                          <Typography variant="body2" color="error">
                            No Airflow task found for this component
                          </Typography>
                        )}
                      </Box>

                      {/* Navigation */}
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Button 
                          onClick={handlePreviousComponent}
                          disabled={currentComponentIndex === 0}
                        >
                          Previous
                        </Button>
                        <Typography variant="body2" color="text.secondary">
                          Component {currentComponentIndex + 1} of {componentsWithSql.length}
                        </Typography>
                        <Button 
                          onClick={handleNextComponent}
                          disabled={currentComponentIndex === componentsWithSql.length - 1}
                        >
                          Next
                        </Button>
                      </Box>
                    </Box>
                  );
                })()}
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setManualSqlDialogOpen(false)}>Cancel</Button>
                <Button 
                  variant="contained" 
                  color="success"
                  onClick={handleFinishManualInjection}
                >
                  Finish & Apply Changes
                </Button>
              </DialogActions>
            </Dialog>

            {/* DAG Preview Dialog */}
            <Dialog 
              open={previewDialogOpen} 
              onClose={() => setPreviewDialogOpen(false)} 
              maxWidth="lg" 
              fullWidth
            >
              <DialogTitle>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <CodeIcon sx={{ mr: 1 }} />
                  Updated DAG Preview
                </Box>
              </DialogTitle>
              <DialogContent>
                <Box sx={{ mt: 2 }}>
                  <Paper sx={{ p: 2, mb: 3, bgcolor: 'success.light', color: 'success.contrastText' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 1 }}>
                      ✓ Manual SQL Injection Complete!
                    </Typography>
                    <Typography variant="body2">
                      Review the updated DAG code below. You can now download the final version.
                    </Typography>
                  </Paper>
                  <Paper>
                    <Box sx={{ maxHeight: 600, overflow: 'auto' }}>
                      <SyntaxHighlighter 
                        language="python"
                        style={materialDark}
                        customStyle={{ margin: 0, borderRadius: 4 }}
                        showLineNumbers={true}
                      >
                        {previewDagCode}
                      </SyntaxHighlighter>
                    </Box>
                  </Paper>
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setPreviewDialogOpen(false)}>Close</Button>
                <Button 
                  variant="contained"
                  color="primary"
                  onClick={handleDownloadUpdatedDag}
                  startIcon={<DownloadIcon />}
                >
                  Download Updated DAG
                </Button>
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
