#!/bin/bash
PROJECT="etl-censo-d"
REGION="us-east1"
BUCKET="gs://etl-censo-df"
BQ_DATASET="ds_censo"
BQ_TABLE="tbl_censo"

python -m gcp \
    --runner DataflowRunner \
    --project $PROJECT \
    --region $REGION \
    --temp_location $BUCKET/temp \
    --staging_location $BUCKET/staging \
    --sdk_location container \
    --save_main_session True \
    --enable_hot_key_logging \
    --dataset $BQ_DATASET \
    --table $BQ_TABLE \
    --input_location $BUCKET/input \
    --output_location $BUCKET/out \
    --machine_type 'c4-standard-4'