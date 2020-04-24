import pprint
import pandas as pd
from .service import Service
from .access import AnalyticApiKey
import os
from durbango import tqdm_nice
import datetime

T = 'Time Spent (seconds)'


class RTAnalyzer:

    def __init__(self, key=None, path=None):
        if key is None:
            key = os.environ['RESCUETIME_KEY']
        self.key = key
        self.path = path

    def fetch(self, **fetch_kwargs):
        self.df = self.fetch_df(**fetch_kwargs)

    @classmethod
    def from_disk(cls, path, key=None):
        obj = cls(key=key, path=path)
        obj.df = pd.read_csv(path)
        return obj

    @property
    def date_range(self):
        return pd.date_range(self.df.date.min(), self.df.date.max())

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

    def fetch_df(self, start_date='2019-09-01', end_date=None) -> pd.DataFrame:
        self.s = Service.Service()
        self.k = AnalyticApiKey.AnalyticApiKey(self.key, self.s)
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        date_lst = [str(d.date()) for d in pd.date_range(start_date, end_date)]
        res = []
        for i in tqdm_nice(range(len(date_lst) - 1)):
            res.append(self._fetch_one(date_lst[i], date_lst[i + 1]))
        df = pd.concat(res)

        return df

    def _add_cols(self):
        _add_cols(self.df)

    def summary_type1(self, tgrouper='qstring', activity_grouper='Activity'):
        return summary_type_1(self.df, tgrouper=tgrouper, activity_grouper=activity_grouper)

    def daily_avg(self, grouper='qstring', df=None):
        _df = df if df is not None else self.df
        return daily_avg(_df, grouper)

def _add_cols(df):
    df['date'] = pd.to_datetime(df.Date)
    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.quarter
    df['qstring'] = df['year'].astype(str) + '-Q' + df.date.dt.quarter.astype(str)
    df['Month'] = df.date.dt.strftime('%Y-%m')
    df['fake_grouper'] = 1


def summary_type_1(df, tgrouper='qstring', activity_grouper='Activity'):
    totals = df.groupby([tgrouper, activity_grouper])[T].sum().rename('hours') / 3600
    unstacked = totals.unstack().fillna(0)
    deltas = unstacked.diff().stack()
    pct_chg = unstacked.pct_change().stack()
    tab = totals.to_frame('hours').assign(chg=deltas, pct_chg=pct_chg)
    # significance filter
    return tab[tab['hours'] > 1]


JUPYTER_ALIASES = {

}


def daily_avg(df, gb='qstring'):
    secs_per_hour = 3600
    day_count = df.groupby(gb).date.nunique()
    hours_per_day = (df.groupby(gb)[T].sum() / day_count) / secs_per_hour
    return hours_per_day
