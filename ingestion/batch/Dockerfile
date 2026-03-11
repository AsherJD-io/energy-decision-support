FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_entsoe.py .
COPY db.py .

ENTRYPOINT ["python", "ingest_entsoe.py"]
