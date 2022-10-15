from unittest import TestCase

from existenz_api_fetcher.hydro import flow, temperature


class Test(TestCase):
    def test_flow(self):
        self.assertIsNone(flow(21299229))

    def test_temperature(self):
        self.assertIsNone(temperature('2119'))

    def test_height(self):
        self.assertIsNone(flow('21299229'))
