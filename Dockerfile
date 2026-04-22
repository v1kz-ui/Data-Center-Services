FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md alembic.ini ./
COPY apps ./apps
COPY workers ./workers
COPY configs ./configs
COPY db ./db
COPY scripts ./scripts

RUN python -m pip install --upgrade pip \
    && python -m pip install .

CMD ["sh", "-c", "python -m uvicorn app.main:app --app-dir apps/api/src --host 0.0.0.0 --port ${PORT:-8000}"]
