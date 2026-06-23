# Build context is the repo root to COPY in shared/contract.py + shared/modeling.py.
# numpy/scikit-learn pins must match the Flink image or the model pickle won't load in the UDF.
FROM python:3.10-slim-bookworm
WORKDIR /app
COPY retrain/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY shared/contract.py ./contract.py
COPY shared/modeling.py ./modeling.py
COPY retrain/ ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
