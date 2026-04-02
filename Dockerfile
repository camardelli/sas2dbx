FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia metadados e código completo antes do pip install
# (hatchling precisa do código-fonte para instalar corretamente)
COPY pyproject.toml README.md ./
COPY sas2dbx/ ./sas2dbx/

# Instala o pacote com dependências web + databricks SDK
RUN pip install --no-cache-dir ".[web,databricks]"

# Diretório de trabalho para migrações (montado como volume em produção)
RUN mkdir -p /data/sas2dbx_work

EXPOSE 8000

ENV WORK_DIR=/data/sas2dbx_work
ENV MAX_UPLOAD_MB=200

CMD ["uvicorn", "sas2dbx.web.app:create_app", \
     "--factory", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1"]
