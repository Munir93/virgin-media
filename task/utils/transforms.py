import apache_beam as beam
import json
import os
from datetime import datetime


class MapElements(beam.DoFn):
    """
        Applies schema to fields for ease of processing
        Args:
            shcema (String): path to schema file
        Returns:
            elements (dict): pcoll of dict rows
    """

    def __init__(self, schema):
        self.schema_path = schema
        self.root_path = os.path.abspath(os.path.join(__file__, "../.."))

    def setup(self):
        with open(f"{self.root_path}{self.schema_path}") as f:
            schema = json.loads(f.read())
        self.fields = [x["name"] for x in schema['fields']]

    def process(self, element):
        to_list = element.split(',')
        yield {self.fields[x]: to_list[x] for x in range(len(self.fields))}


def filter_transactions(element):
    """
    Filters out elements with a transaction ammount less than 20
    """
    element['transaction_ammount'] = float(element['transaction_ammount'])
    return element['transaction_ammount'] > 20


def filter_date(element):
    """
    Filters out elements with a date before 2010
    """
    return datetime.strptime(element['timestamp'],
                             "%Y-%m-%d %H:%M:%S %Z").year >= 2010


def create_kv(element):
    """
    Creates a key value (tuple) from the year and transaction ammount
    """

    year_date = datetime.strptime(
        element['timestamp'], "%Y-%m-%d %H:%M:%S %Z").year
    return year_date, element["transaction_ammount"]


class CompositeTransform(beam.PTransform):
    """
    Composite transform that combines the processing steps of the pervious transforms
    """

    def __init__(self, schema):
        self.schema_path = schema

    def expand(self, pcoll):
        return (pcoll | beam.ParDo(MapElements(self.schema_path))
                      | beam.Filter(filter_transactions)
                      | beam.Filter(filter_date)
                      | beam.Map(create_kv)
                      | beam.CombinePerKey(sum))
