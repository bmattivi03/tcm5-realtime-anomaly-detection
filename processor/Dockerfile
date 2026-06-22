# Build context is the repo root so shared/contract.py + shared/modeling.py are COPYed in
# alongside the processor sources and the 3 connector JARs.
ARG FLINK_IMAGE=fracorco/pyflink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}

WORKDIR /app
COPY processor/requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt

COPY shared/contract.py ./contract.py
COPY shared/modeling.py ./modeling.py
COPY processor/ ./

# Submit the job to the Flink cluster. The shared modules (contract/modeling/model_udf)
# are importable from /app on the submit side and baked into the Flink image on the
# worker side, so no -pyfs shipping is needed (shipping the dir was unreliable).
CMD flink run \
    -m ${PROCESSOR_JOBMANAGER:-localhost:8081} \
    -py ${PROCESSOR_IMPLEMENTATION:-processor.py} \
    $PROCESSOR_ARGS
