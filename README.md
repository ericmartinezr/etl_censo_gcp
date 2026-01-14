# etl_censo_gcp

ETL con datos del censo de Chile

# Configuración GCP

```bash
export PROJECT_ID="etl-censo"
export REGION="us-central1"
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
export BQ_DATASET="ds_censo"
export BQ_TABLE="tbl_censo"

gcloud config set project $PROJECT_ID
```

## Habilitar APIs necesarias

```bash
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    compute.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com
```

## Crear bucket

Crear el bucket para almacenar los archivos del pipeline.

```bash
BUCKET_NAME="etl-censo-df"

gcloud storage buckets create gs://${BUCKET_NAME} \
    --location=$REGION \
    --uniform-bucket-level-access

# Crear estructura de directorios
gcloud storage folders create gs://${BUCKET_NAME}/input
gcloud storage folders create gs://${BUCKET_NAME}/out
gcloud storage folders create --recursive gs://${BUCKET_NAME}/temp/staging
gcloud storage folders create gs://${BUCKET_NAME}/templates


# Listar los directorios para validar su creacion
gcloud storage folders list gs://${BUCKET_NAME}/
```

Las entradas parquet se pueden descargar desde el enlace https://censo2024.ine.gob.cl/resultados/.

Los archivos de entrada deberían quedar de la siguiente forma:

```
gs://<bucket>/input/censo_schema.json
gs://<bucket>/input/codigos_otros.csv
gs://<bucket>/input/codigos_territoriales.csv
gs://<bucket>/input/hogares_censo2024.parquet
gs://<bucket>/input/personas_censo2024.parquet
gs://<bucket>/input/viviendas_censo2024.parquet
```

## Crear Artifact Registry

Crear el repositorio para almacenar la imagen Docker

```bash
gcloud artifacts repositories create censo-artifact-repo \
    --repository-format=docker \
    --location=$REGION \
    --description="Repositorio Censo2024" \
    --disable-vulnerability-scanning
```

## Crear una cuenta de servicio para CloudBuild

```sh
gcloud iam service-accounts create cloudbuild-app-sa \
  --description="Cuenta de servicio para CloudBuild"

SA_EMAIL_CB="cloudbuild-app-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/cloudbuild.builds.editor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/artifactregistry.writer"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/storage.objectViewer"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/logging.logWriter"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/dataflow.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_CB}" --role="roles/iam.serviceAccountUser"
```

## Crear un trigger para Cloud Build

Se asume que la conexión al repositorio ya se realizó.

Nota: Al enlazar un repositorio GCP da 2 opciones para "Repository name":

1. Generated
2. Manual

Cualquiera que se elija, el nombre que genere debe ser asignado en la opción `--repository` en la última sección `.../repositories/<NOMBRE_GENERADO_AL_ENLAZAR>`

```sh
# La conexion debe estar creada
GIT_CONN="github-connection"
GIT_REPO="etl_censo_gcp"

gcloud builds triggers create github \
  --name="censo-trigger" \
  --repository="projects/${PROJECT_ID}/locations/${REGION}/connections/${GIT_CONN}/repositories/ericmartinezr-${GIT_REPO}" \
  --branch-pattern="^master$" \
  --build-config="cloudbuild.yaml" \
  --region=${REGION} \
  --service-account="projects/${PROJECT_ID}/serviceAccounts/${SA_EMAIL_CB}"
```

## Crear una cuenta de servicio para Dataflow

```sh
gcloud iam service-accounts create dataflow-app-sa \
  --description="Cuenta de servicio para Dataflow"

SA_EMAIL_AF="dataflow-app-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/storage.objectUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/bigquery.user"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/logging.logWriter"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/dataflow.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/monitoring.metricWriter"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA_EMAIL_AF}" --role="roles/artifactregistry.writer"
```

## Crear dataset y tabla en BigQuery

```sh
bq mk --dataset --location=$REGION $PROJECT_ID:$BQ_DATASET
bq mk --table --clustering_fields=sexo,edad,p23_est_civil \
--description="Tabla con datos del Censo 2024" \
--schema=./censo_schema.json \
$PROJECT_ID:$BQ_DATASET.$BQ_TABLE
```

## Ejecución

1. El job se ejecuta una vez que se haga un push a la rama especificada al crear el trigger
2. Ejecución manual desde Cloud Shell (esto ejecuta la última imagen que se haya generado del build anterior)

```sh
gcloud dataflow flex-template run etl-censo-job-01 \
--template-file-gcs-location gs://etl-censo-df/templates/censo-pipeline.json \
--region us-central1 \
--worker-region us-central1 \
--launcher-machine-type e2-standard-2 \
--worker-machine-type e2-standard-2 \
--parameters project=etl-censo,region=us-central1,dataset=ds_censo,table=tbl_censo,input_location=gs://etl-censo-df/input,staging_location=gs://etl-censo-df/temp/staging,output_location=gs://etl-censo-df/out,job_name=etl-censo-job-01,service_account_email=dataflow-app-sa@etl-censo.iam.gserviceaccount.com
```

## Referencia

- https://docs.cloud.google.com/bigquery/docs/partitioned-tables
- https://docs.cloud.google.com/bigquery/docs/clustered-tables
- https://docs.cloud.google.com/bigquery/docs/schemas
- https://docs.cloud.google.com/bigquery/docs/nested-repeated
- https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format
- https://docs.cloud.google.com/dataflow/docs/concepts/security-and-permissions
- https://docs.cloud.google.com/dataflow/docs/guides/templates/using-flex-templates
- https://docs.cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#python
