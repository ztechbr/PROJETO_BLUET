FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8001

COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove git && rm -rf /var/lib/apt/lists/*

COPY app.py database.py leituras_query.py soap_service.py ./

EXPOSE 8001

CMD ["sh", "-c", "python app.py"]
