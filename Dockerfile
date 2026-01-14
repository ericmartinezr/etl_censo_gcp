# Template base Flex, para obtener el binario del launcher
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:flex_templates_base_image_release_20260112_RC00 as template_launcher

# Apache Beam SDK
FROM apache/beam_python3.12_sdk:2.70.0

ARG WORKDIR=/template
WORKDIR ${WORKDIR}


COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY gcp.py .
COPY requirements.txt .

#  TODO: Validar si es necesario, no se instalo nada aparte de apache beam
RUN pip install --no-cache-dir -r requirements.txt

ARG VAR_PROJECT="etl-censo"
ARG VAR_REGION="us-central1"
ARG VAR_BUCKET_TEMP="gs://etl-censo-df/temp"

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/gcp.py"
ENV GCP_PROJECT=${VAR_PROJECT}
ENV GCP_BUCKET_INPUT="gs://etl-censo-df/input"
ENV GCP_BUCKET_TEMP="${VAR_BUCKET_TEMP}"
ENV GCP_BUCKET_OUTPUT="gs://etl-censo-df/out"
ENV GCP_DATASET="ds_censo"
ENV GCP_TABLE="tbl_censo"

# Group all your pipeline settings into this one variable
ENV FLEX_TEMPLATE_PYTHON_PY_OPTIONS="--runner=DataflowRunner \
--project=etl-censo \
--region=us-central1 \
--temp_location=gs://etl-censo-df/temp \
--staging_location=gs://etl-censo-df/temp/staging \
--service_account_email=dataflow-app-sa@etl-censo.iam.gserviceaccount.com \
--job_name=etl-censo-job-01 \
--streaming=False \
--enable_hot_key_logging=True \
--save_main_session=True \
--sdk_location=container"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]