import pprint
import pandas as pd
from .service import Service
from .access import AnalyticApiKey
import os
from durbango import tqdm_nice
import datetime

T = 'Time Spent (seconds)'


class RTAnalyzer:

    def __init__(self, key=None):
        self.s = Service.Service()
        if key is None:
            key = os.environ['RESCUETIME_KEY']
        self.k = AnalyticApiKey.AnalyticApiKey(ENV_KEY, key)

    def _fetch_one(self, start_date, end_date):
        p = {}
        p['restrict_begin'] = start_date
        p['restrict_begin'] = end_date
        p['restrict_kind'] = 'activity'
        p['perspective'] = 'interval'
        p['resolution_time'] = 'day'
        d = self.s.fetch_data(self.k, p)
        df = pd.DataFrame(d['rows'], columns=d['row_headers'])
        print(f'from {df.Date.min()} to {df.Date.max()}')
        return df

    def fetch_df(self, start_date = '2019-09-01', end_date=None) -> pd.DataFrame:
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        date_lst = [str(d.date()) for d in pd.date_range(start_date, end_date)]
        res = []
        for i in tqdm_nice(range(len(date_lst)-1)):
            res.append(self._fetch_one(date_lst[i], date_lst[i + 1]))
        df = pd.concat(res)
        return df

    @staticmethod
    def _add_cols(df):
        df['date'] = pd.to_datetime(df.Date)
        df['year'] = df.date.dt.year
        df['month'] = df.date.dt.quarter
        df['qstring'] = df['year'].astype(str) + '-Q' + df.date.dt.quarter.astype(str)
        df['Month'] = df.date.dt.strftime('%Y-%m')


def daily_avg(df, gb='qstring'):
    secs_per_hour = 3600
    day_count = df.groupby(gb).date.nunique()
    hours_per_day = (df.groupby(gb)[T].sum() / day_count) / secs_per_hour
    return hours_per_day
