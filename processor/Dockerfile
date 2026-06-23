# build context is the repo root so shared/contract.py + shared/modeling.py COPY in
ARG FLINK_IMAGE=fracorco/pyflink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}

WORKDIR /app
COPY processor/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt

COPY shared/contract.py ./contract.py
COPY shared/modeling.py ./modeling.py
COPY processor/ ./

# shared modules import from /app on submit and are baked into the image on workers,
# so no -pyfs (dir shipping was unreliable)
CMD flink run \
    -m ${PROCESSOR_JOBMANAGER:-localhost:8081} \
    -py ${PROCESSOR_IMPLEMENTATION:-processor.py} \
    $PROCESSOR_ARGS
