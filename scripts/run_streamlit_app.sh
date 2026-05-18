#!/bin/bash

echo "================================"
echo "START STREAMLIT DASHBOARD"
echo "================================"

docker rm -f streamlit-dashboard 2>/dev/null || true

docker run --rm -it 
--name streamlit-dashboard 
--network=streaming-network 
-p 8501:8501 
-v $(pwd):/app 
python:3.10 bash -c "

cd /app &&

pip install --no-cache-dir 
streamlit 
pandas 
plotly 
sqlalchemy 
psycopg2-binary &&

streamlit run dashboard/app.py 
--server.address 0.0.0.0 
--server.port 8501
"
