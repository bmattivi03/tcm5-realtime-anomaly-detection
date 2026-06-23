# PyFlink + the ML deps the scoring UDF imports; UDF runs on the TaskManager so they
# must be present here. numpy/pandas/scikit-learn pinned identically to the retrain
# sidecar or the pickled model fails to load.
ARG FLINK_IMAGE=fracorco/pyflink:1.18.1-scala_2.12-java11
FROM ${FLINK_IMAGE}
COPY processor/requirements.txt /tmp/requirements.txt
RUN pip install -U pip && pip install -r /tmp/requirements.txt && rm -rf /root/.cache

# Bake onto the UDF worker's import path: closures and the pickled model reference these
# modules by name at unpickle time, so they must be importable (-pyfs is unreliable).
COPY shared/contract.py shared/modeling.py processor/model_udf.py processor/unsupervised.py /tmp/tcm5/
RUN cp /tmp/tcm5/contract.py /tmp/tcm5/modeling.py /tmp/tcm5/model_udf.py /tmp/tcm5/unsupervised.py \
      "$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')/"
