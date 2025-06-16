import React, { useState } from 'react';
import { Button, List, ListItem, ListItemText, Drawer, AppBar, Toolbar, Typography, Box } from '@mui/material';
import { ReactFlowProvider, ReactFlow } from 'react-flow-renderer';

function findSql(text, component) {
  const pattern = new RegExp(component + "[\\s\\S]{0,200}?<pre>([\\s\\S]*?)<\\/pre>", "i");
  const m = pattern.exec(text);
  if (m) return m[1].trim();
  const generic = /SELECT[\s\S]*?;/i.exec(text);
  return generic ? generic[0].trim() : '';
}

function parseJobFile(file) {
  return file.text().then(text => {
    const doc = new DOMParser().parseFromString(text, 'text/html');
    const jobName = (doc.querySelector('title') || {}).textContent || file.name;
    const componentMatches = text.match(/t[A-Za-z0-9_]+/g) || [];
    const components = Array.from(new Set(componentMatches)).map(name => ({
      id: name,
      name,
      type: name,
      sql: findSql(text, name),
      children: []
    }));

    const connections = [];
    const connectionRegex = /([tA-Za-z0-9_]+)\s*--?>\s*([tA-Za-z0-9_]+)/g;
    let m;
    while ((m = connectionRegex.exec(text))) {
      connections.push({ from: m[1], to: m[2] });
    }
    if (!connections.length) {
      for (let i = 0; i < components.length - 1; i++) {
        connections.push({ from: components[i].id, to: components[i + 1].id });
      }
    }

    // child job names (very naive extraction)
    const childRegex = /tRunJob[^A-Za-z0-9_]+([A-Za-z0-9_]+)/gi;
    components.forEach(c => {
      if (/tRunJob|joblet/i.test(c.name)) {
        const childNames = [];
        let cm;
        while ((cm = childRegex.exec(text))) {
          if (cm[1]) childNames.push(cm[1]);
        }
        c.children = childNames;
      }
    });

    return { name: jobName, components, connections };
  });
}

function App() {
  const [jobs, setJobs] = useState([]);
  const [selectedJob, setSelectedJob] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [expanded, setExpanded] = useState({});

  const handleFiles = async (e) => {
    const files = Array.from(e.target.files).filter(f => f.name.endsWith('.html'));
    const parsed = await Promise.all(files.map(parseJobFile));
    setJobs(parsed);
    setSelectedJob(null);
    setExpanded({});
  };

  const onNodeClick = (event, node) => {
    setSelectedNode(node);
    if (/tRunJob|joblet/i.test(node.data.type)) {
      setExpanded(prev => ({ ...prev, [node.id]: !prev[node.id] }));
    }
  };

  const generateDag = () => {
    if (!selectedJob) return;
    const lines = [];
    lines.push('from airflow import DAG');
    lines.push('from airflow.operators.python import PythonOperator');
    lines.push('from datetime import datetime');
    lines.push('from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook');
    lines.push('');
    lines.push('def get_secret(secret_id):');
    lines.push('    hook = AwsBaseHook(client_type="secretsmanager")');
    lines.push('    return hook.get_conn().get_secret_value(SecretId=secret_id)["SecretString"]');
    lines.push('');
    lines.push(`with DAG("${selectedJob.name}", start_date=datetime(2023,1,1), schedule_interval=None) as dag:`);
    selectedJob.components.forEach(c => {
      lines.push(`    def task_${c.id}():`);
      if (c.sql) {
        lines.push('        sql = """' + c.sql.replace(/"""/g, '\\"\\"\\"') + '"""');
        lines.push('        print(sql)');
      } else if (/connection/i.test(c.name)) {
        lines.push('        creds = get_secret("my_secret")  # TODO: update secret id');
        lines.push('        print(creds)');
      } else {
        lines.push('        pass');
      }
      lines.push('');
      lines.push(`    op_${c.id} = PythonOperator(task_id='${c.id}', python_callable=task_${c.id})`);
    });
    selectedJob.connections.forEach(conn => {
      lines.push(`    op_${conn.from} >> op_${conn.to}`);
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = selectedJob.name.replace(/\s+/g, '_') + '_dag.py';
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
            <input type="file" webkitdirectory="true" multiple hidden onChange={handleFiles} />
          </Button>
          {selectedJob && <Button color="inherit" onClick={generateDag}>Download DAG</Button>}
        </Toolbar>
      </AppBar>
      <Box sx={{ display: 'flex', height: 'calc(100% - 64px)' }}>
        <Box sx={{ width: 250, overflow: 'auto', borderRight: '1px solid #ccc' }}>
          <List>
            {jobs.map(job => (
              <ListItem button key={job.name} selected={selectedJob === job} onClick={() => setSelectedJob(job)}>
                <ListItemText primary={job.name} />
              </ListItem>
            ))}
          </List>
        </Box>
        <Box sx={{ flexGrow: 1 }}>
          {selectedJob && (
            <ReactFlowProvider>
              <ReactFlow
                nodes={(function() {
                  const base = selectedJob.components.map((c, idx) => ({
                    id: c.id,
                    data: { label: c.name, type: c.type, sql: c.sql },
                    position: { x: 100 * idx, y: 50 }
                  }));
                  // add expanded children
                  selectedJob.components.forEach((c, idx) => {
                    if (expanded[c.id]) {
                      c.children.forEach((child, ci) => {
                        base.push({
                          id: `${c.id}_${child}`,
                          data: { label: child, type: 'child' },
                          position: { x: 100 * ci, y: 150 + 50 * idx }
                        });
                      });
                    }
                  });
                  return base;
                })()}
                edges={(function() {
                  const edgeList = selectedJob.connections.map((conn, i) => ({
                    id: `e${i}`,
                    source: conn.from,
                    target: conn.to
                  }));
                  selectedJob.components.forEach(c => {
                    if (expanded[c.id]) {
                      c.children.forEach(child => {
                        edgeList.push({
                          id: `e${c.id}-${child}`,
                          source: c.id,
                          target: `${c.id}_${child}`
                        });
                      });
                    }
                  });
                  return edgeList;
                })()}
                onNodeClick={onNodeClick}
                style={{ width: '100%', height: '100%' }}
              />
            </ReactFlowProvider>
          )}
        </Box>
        <Drawer anchor="right" open={Boolean(selectedNode)} onClose={() => setSelectedNode(null)}>
          {selectedNode && (
            <Box sx={{ width: 250, p: 2 }}>
              <Typography variant="h6" gutterBottom>{selectedNode.data.label}</Typography>
              <Typography variant="body2" gutterBottom>Type: {selectedNode.data.type}</Typography>
              {selectedNode.data.sql && (
                <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{selectedNode.data.sql}</pre>
              )}
            </Box>
          )}
        </Drawer>
      </Box>
    </Box>
  );
}

export default App;
