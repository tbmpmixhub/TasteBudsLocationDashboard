#!/bin/bash

# Start FastAPI token server on port 8000
uvicorn token_server:app --host 0.0.0.0 --port 8000 &

# Start Streamlit on DO's assigned port
streamlit run main.py --server.port $PORT --server.address 0.0.0.0