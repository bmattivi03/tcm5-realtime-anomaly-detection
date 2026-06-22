# Flink JobManager/TaskManager image = PyFlink 1.18.1 + the ML deps the scoring UDF
# imports (scikit-learn/scipy/joblib). The UDF executes on the TaskManager, so these
# must be present there. Same pinned numpy/pandas/scikit-learn as the retrain sidecar,
# so the pickled model loads cleanly.
ARG FLINK_IMAGE=fracorco/pyflink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}
COPY processor/requirements.txt /tmp/requirements.txt
RUN pip install -U pip && pip install -r /tmp/requirements.txt && rm -rf /root/.cache

# Make contract + modeling + the scoring UDFs importable by the Python UDF worker:
# the UDF closures and the pickled model reference these modules BY NAME at unpickle
# time, so they must be on the worker's import path (shipping via -pyfs is unreliable).
COPY shared/contract.py shared/modeling.py processor/model_udf.py processor/unsupervised.py /tmp/tcm5/
RUN cp /tmp/tcm5/contract.py /tmp/tcm5/modeling.py /tmp/tcm5/model_udf.py /tmp/tcm5/unsupervised.py \
      "$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')/"
