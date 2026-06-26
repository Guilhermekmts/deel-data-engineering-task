FROM apache/spark:3.5.1-python3

USER root

# Python libraries for the pipeline + notebook
RUN pip install --no-cache-dir \
    kafka-python==2.2.15 \
    psycopg2-binary==2.9.10 \
    delta-spark==3.2.0 \
    notebook==7.2.3 \
    jupyterlab==4.2.5

ENV PATH="${PATH}:/opt/spark/bin"

RUN ln -sf /opt/spark/bin/spark-submit /usr/local/bin/spark-submit

RUN mkdir -p /home/spark/.local/share/jupyter/runtime && \
    chown -R spark:spark /home/spark

RUN mkdir -p /workspace /workspace/.spark-checkpoints /workspace/data/delta

WORKDIR /workspace

USER spark
