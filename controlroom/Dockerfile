# build context is the repo root (same as the other services)
# read-only Postgres view + SSE fan-out, so no ML deps here
FROM python:3.10-slim-bookworm
WORKDIR /app
COPY controlroom/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY controlroom/ ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
