FROM python:3.12-slim

RUN pip install --no-cache-dir \
    streamlit==1.41.1 \
    psycopg2-binary==2.9.10 \
    pandas==2.2.3 \
    deltalake==1.6.1 \
    streamlit-autorefresh==1.0.1

WORKDIR /app
COPY dashboard/ .

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
