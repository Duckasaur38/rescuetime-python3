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
            try:
                key = os.environ['RESCUETIME_KEY']
            except KeyError:
                print('cant find key, no big deal. set self.key to fetch')
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

    @property
    def dummy_df(self):
        dummy_df = pd.DataFrame(dict(Date=self.date_range)).assign(T=0)
        _add_cols(dummy_df)
        return dummy_df

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

    def refresh(self):
        #pd.date_range('2020-01-01', today)
        raise NotImplementedError('partial dates?')

    def clean_df(self):
        _add_cols(self.df)
        update_jupyter_aliases_(self.df)
        activity_21 = self.df['Activity']
        activity_21.loc[~self.df.Activity.isin(FREQ)] = 'Misc.'
        self.df['activity_21'] = activity_21

    def summary_type1(self, tgrouper='qstring', activity_grouper='Activity'):
        return summary_type_1(self.df, tgrouper=tgrouper, activity_grouper=activity_grouper)

    def daily_avg(self, grouper='qstring', df=None):
        _df = df if df is not None else self.df
        day_count = self.dummy_df.groupby(grouper)['date'].nunique()
        hours = daily_avg(_df, grouper)
        avg = hours/day_count
        return avg

    def pct_of_days(self, slice, tgrouper):
        day_count = self.dummy_df.groupby(tgrouper)['date'].nunique()
        ndays = slice.groupby(tgrouper)['date'].nunique()
        return ndays.to_frame('n_days').assign(frac=ndays/day_count)

    def productivity_cube(self, tgrouper='Month', col='Productivity'):
        cube = self.groupby([tgrouper, col])[T].sum().unstack()
        return (cube / 3600).round()

    def __getattr__(self, item):
        if item in self.__dict__:
            return getattr(self, item)
        else:
            return getattr(self.df, item)

    def __getitem__(self, item):
        return self.df.__getitem__(item)

    def __setitem__(self, item):
        return self.df.__setitem__(item)
FREQ = ['Evernote', 'Google Documents', 'Google Presentations',
        'Google Spreadsheets', 'Jupyter Notebook', 'Preview', 'Slack',
        'amazon.com', 'arxiv.org', 'espn.com', 'github.com', 'google chrome',
        'google-chrome', 'google.com', 'google.com/calendar',
        'inbox.google.com', 'iterm2', 'kaggle.com', 'messages', 'netflix.com',
        'newtab', 'overleaf.com', 'phabricator.kensho.com', 'piazza.com',
        'pycharm', 'quip', 'stanford-pilot.hosted.panopto.com', 'sublime text',
        'superhuman', 'twitter.com', 'youtube.com']
def _add_cols(df):
    df['date'] = pd.to_datetime(df.Date)
    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.quarter
    df['qstring'] = df['year'].astype(str) + '-Q' + df.date.dt.quarter.astype(str)
    df['Month'] = df.date.dt.strftime('%Y-%m')
    df['fake_grouper'] = 1

def productivity_cube(self, tgrouper='Month', col='Productivity'):
    cube = self.groupby([tgrouper, col])[T].sum().unstack()
    return (cube / 3600).round()

def update_jupyter_aliases_(df):
    mask = df.Activity.str.startswith('localhost')
    for suffix in [':8888', ':8889', ':8890', ':5555']:
        mask = mask | df.Activity.str.endswith(suffix)
    df.loc[mask, 'Activity']= 'Jupyter Notebook'
    df.loc[df['Activity'] == 'Jupyter Notebook', 'Category'] = 'Jupyter Notebook'
    df['Activity'] = df['Activity'].str.replace('google chrome', 'google-chrome')

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
    return df.groupby(gb)[T].sum() / secs_per_hour # hours
