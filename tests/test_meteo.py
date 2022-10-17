from unittest import TestCase

import existenz_api_fetcher as ef


class Test(TestCase):
    def test_rainfall(self):
        self.assertIsNone(ef.rainfall(2))

    def test_temperature(self):
        self.assertIsNone(ef.temperature(56337))

    def test_humidity(self):
        self.assertIsNone(ef.humidity(4577))

    def test_wind_speed(self):
        self.assertIsNone(ef.wind_speed(4321))

    def test_radiation(self):
        self.assertIsNone(ef.radiation(22))

    def test_pressure(self):
        self.assertIsNone(ef.pressure(3456435))

    def test_sunshine_duration(self):
        self.assertIsNone(ef.sunshine_duration('9827'))

    def test_dew_point(self):
        self.assertIsNone(ef.dew_point(145315))

    def test_ev_penman(self):
        self.assertIsNone(ef.ev_penman('0', 500, 40))

    def test_ev_penman_monteith(self):
        self.assertIsNone(ef.ev_penman_monteith('3211', 500, 40),)

    def test_ev_fao56(self):
        self.assertIsNone(ef.ev_fao56('12', 500, 40))
