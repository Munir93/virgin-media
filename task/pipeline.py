from __future__ import absolute_import
import argparse
import yaml
import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from task.utils.transforms import (
    MapElements,
    filter_date,
    filter_transactions,
    create_kv,
    CompositeTransform
)


root_path = os.path.dirname(os.path.abspath(__file__))


def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', dest="config_file", required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with open(f"{root_path}{known_args.config_file}") as config_file:
        config = yaml.safe_load(config_file)

    schema = config["schema_file"]
    gcs_file = config["gcs_file"]
    outfile = config['outfile']
    suffix = config['suffix']
    COMPOSITE = 'comp'

    p = beam.Pipeline(options=pipeline_options)

    input_data = (
        p | "Read from GCS" >> beam.io.ReadFromText(
            gcs_file, skip_header_lines=1))

    output = (input_data
              | "Map elements" >> beam.ParDo(MapElements(schema))
              | "Filter trans" >> beam.Filter(filter_transactions)
              | "Filter date" >> beam.Filter(filter_date)
              | "KV pairs" >> beam.Map(create_kv)
              | "Sum By Year" >> beam.CombinePerKey(sum)
              | "Write to local" >> beam.io.WriteToText(outfile, suffix)
              )

    composite_output = (
        input_data
        | "Composite Transform" >> CompositeTransform(schema)
        | "Write to local2" >> beam.io.WriteToText(outfile+COMPOSITE, suffix)
    )

    execute = p.run()
    execute.wait_until_finish()
