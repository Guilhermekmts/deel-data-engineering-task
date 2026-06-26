#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo "Ensuring Jupyter runtime directories exist..."
docker compose exec spark bash -lc "mkdir -p /home/spark/.local/share/jupyter/runtime /tmp/jupyter-runtime"

echo "Starting Jupyter Notebook inside the spark container..."
docker compose exec -d spark bash -lc "JUPYTER_RUNTIME_DIR=/tmp/jupyter-runtime jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.token='deel' --ServerApp.password='' --NotebookApp.token='deel' --NotebookApp.open_browser=False --notebook-dir=/workspace/notebooks --ServerApp.allow_origin='*' --ServerApp.allow_root=True"

echo "Notebook starting..."
echo "Open: http://localhost:8888/?token=deel"
echo ""
echo "If the page is not ready yet, wait a few seconds and retry."
echo "To check status: docker compose exec spark jupyter notebook list"
echo "To stop: docker compose exec spark pkill -f jupyter"
