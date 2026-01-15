import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.filesystems import FileSystems

# Mock your options
options = PipelineOptions()


def test_pipeline():
    # Load your schema string exactly how you fixed it
    with open("censo_schema.json") as f:
        with FileSystems.open("censo_schema.json") as f:
            schema_str = json.loads(f.read().decode('utf-8'))

    # Create 1 single sample record that matches your complex nested structure
    sample_data = [{
        "id_vivienda": 1,
        "id_hogar": 1,
        "id_persona": 1,
        # "tipo_operativo": "TEST",
        "hogar": {
            #  "tipo_operativo": "NESTED_TEST",
            "ubicacion": {"region": "Metropolitana"}
        }
    }]

    with beam.Pipeline(runner='DirectRunner', options=options) as p:
        (p
         | "CreateSample" >> beam.Create(sample_data)
         | "WriteTest" >> beam.io.WriteToBigQuery(
             table="etl-censo:ds_censo.tbl_censo",
             schema={"fields": schema_str},
             method="STORAGE_WRITE_API",  # This triggers the RowCoder
             test_client=None,  # Forces Beam to go through the encoding logic
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         ))


if __name__ == "__main__":
    test_pipeline()
