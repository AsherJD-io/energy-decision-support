FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingest_entsoe.py /app/
COPY db.py /app/

ENTRYPOINT ["python", "/app/ingest_entsoe.py"]
