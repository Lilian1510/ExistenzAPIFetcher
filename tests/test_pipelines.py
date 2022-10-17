from unittest import TestCase

from existenz_api_fetcher import pipelines, meteo


class Test(TestCase):
    def test_merge(self):
        self.assertIsNone(pipelines.merge(meteo.rainfall('756')))

    def test_compute(self):
        self.assertIsNone(pipelines.compute(meteo.rainfall('756')))
