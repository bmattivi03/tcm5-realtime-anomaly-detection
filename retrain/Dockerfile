# Build context is the repo root so shared/contract.py + shared/modeling.py are COPYed in.
# Same pinned numpy/scikit-learn as the Flink image, so the model it writes
# loads cleanly in the UDF.
FROM python:3.10-slim-bookworm
WORKDIR /app
COPY retrain/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY shared/contract.py ./contract.py
COPY shared/modeling.py ./modeling.py
COPY retrain/ ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
