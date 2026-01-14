# Template base Flex, para obtener el binario del launcher
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base AS template_launcher

# Apache Beam SDK
FROM apache/beam_python3.12_sdk:2.70.0

ARG WORKDIR=/template
WORKDIR ${WORKDIR}


COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY gcp.py .
COPY requirements.txt .

RUN apt-get update && apt-get install -y openjdk-17-jdk libffi-dev git && rm -rf /var/lib/apt/lists/* \
&& pip install --no-cache-dir --upgrade pip \
&& pip install --no-cache-dir -r /template/requirements.txt \
&& pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r /template/requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/gcp.py"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]