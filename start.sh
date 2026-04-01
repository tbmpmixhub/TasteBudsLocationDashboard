#!/bin/bash

# Start FastAPI token server on port 8000
uvicorn token_server:app --host 0.0.0.0 --port 8000 &

# Start Streamlit on port 8501
streamlit run main.py --server.port 8501 --server.address 0.0.0.0