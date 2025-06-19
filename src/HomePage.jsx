import React from 'react';
import { Button, Typography, Box } from '@mui/material';
import { useNavigate } from 'react-router-dom';

export default function HomePage() {
  const navigate = useNavigate();
  return (
    <Box sx={{ textAlign: 'center', mt: 10 }}>
      <img src="https://cdn-icons-png.flaticon.com/512/5968/5968705.png" alt="Talend2AirDAG" width={80} />
      <Typography variant="h3" sx={{ mt: 2, fontWeight: 700 }}>
        Talend2AirDAG
      </Typography>
      <Typography variant="h6" sx={{ mt: 2, mb: 4 }}>
        Migrate your Talend jobs to Airflow DAGs with ease.
      </Typography>
      <Button
        variant="contained"
        color="primary"
        size="large"
        onClick={() => navigate('/migrate')}
      >
        Start Migration
      </Button>
    </Box>
  );
} 