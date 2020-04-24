import unittest
import sys
sys.path.append('./')
sys.path.append('../')
from api.reporting import RTAnalyzer

class TestAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rt = RTAnalyzer.from_disk('tests/test_data.csv')
        return cls


    def test_daily_avg(self):
        self.rt.daily_avg('qstring')
        self.rt.daily_avg('Month')

    def test_summary(self):
        self.rt.summary_type1()
