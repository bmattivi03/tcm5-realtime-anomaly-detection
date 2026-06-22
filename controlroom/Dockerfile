# Build context is the repo root, like the other services.
# Read-only view over Postgres + SSE fan-out; no ML deps needed here.
FROM python:3.10-slim-bookworm
WORKDIR /app
COPY controlroom/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY controlroom/ ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
