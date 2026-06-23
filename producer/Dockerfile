# build context is repo root so shared/contract.py is reachable
FROM python:3.10-slim-bookworm
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY producer/ ./
COPY shared/contract.py ./contract.py
CMD ["python3", "producer.py"]
