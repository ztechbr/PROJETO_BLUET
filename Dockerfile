FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8001

COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove git && rm -rf /var/lib/apt/lists/*

# Projeto inteiro (.dockerignore corta lixo como .venv e .git) — evita ficar sem `server/`
COPY . .

# Falha já no build se o pacote MVC não está acessível
RUN python -c "from server import create_app; create_app(); print('import ok')"

EXPOSE 8001

CMD ["python", "app.py"]
