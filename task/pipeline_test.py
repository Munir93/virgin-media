from __future__ import absolute_import
import logging
import apache_beam as beam
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import is_not_empty, equal_to, assert_that
from task.utils.transforms import CompositeTransform


class CompositeTransformTest(unittest.TestCase):

    def setUp(self):
        self.schema = '/schemas/schema.json'
        self.test_data = ["2009-01-09 02:54:25 UTC,wallet00000,wallet0000,10000.7",
                          "2017-01-10 04:22:23 UTC,wallet00000,wallet0000,19.95",
                          "2017-03-19 14:09:16 UTC,wallet,wallet00001e49,108.7",
                          "2017-03-19 14:10:44 UTC,wallet,wallet00000,98.6"]

    def test_not_empty(self):

        with TestPipeline() as p:
            pcoll = (
                p | beam.Create(self.test_data)
                | CompositeTransform(self.schema)
            )

            assert_that(pcoll, is_not_empty())

    def test_desired_result(self):

        with TestPipeline() as p:
            pcoll = (
                p | beam.Create(self.test_data)
                | CompositeTransform(self.schema)
            )

            assert_that(pcoll, equal_to([(2017, 207.3)]))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
