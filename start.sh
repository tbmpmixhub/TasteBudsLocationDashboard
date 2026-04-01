#!/bin/bash

# Start Streamlit on internal port 8501
streamlit run main.py --server.port 8501 --server.address 0.0.0.0 &

# Start FastAPI on the public port 8080
uvicorn token_server:app --host 0.0.0.0 --port 8080