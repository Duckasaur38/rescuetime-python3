import sys
import datetime

sys.path.append('./')
sys.path.append('../')

from api.service.Service import Service
from api.access.AnalyticApiKey import AnalyticApiKey
from api.model.ResponseData import ResponseData

# replace with your own, otherwise nothing will be returned.
apikey = 'B63C4fZ0bQb6iu1EXYPMhsFLT_VqUhutPiu9fIwP'

def test1():
    s = Service()
    s.debug(s.server_loc)
    k = AnalyticApiKey(apikey, s)
    s.debug('exists: ' + str(k.exists()))
    r = ResponseData(k, **{'op': 'select',
                           'vn': 0,
                           'pv': 'rank',
                           'rb': (datetime.date.today() - datetime.timedelta(weeks = 1)).strftime('%Y-%m-%d'),
                           're': datetime.date.today().strftime('%Y-%m-%d'),
                           'rk': 'overview'
                       })
    r.sync()
    s.debug('data object: ' + str(r.object))
    s.debug('=============')
    for k in r.object:
        s.debug("\n\tkey: %s\n\tvalue:%s\n-------\n" % (str(k), str(r.object[k])))
        if k == 'rows':
            for ro in r.object[k]:
                s.debug("\n\t\trow(%d): %s\n-------\n" % (len(ro), str(ro)))

def test2():
    s = Service()
    s.debug(s.server_loc)
    k = AnalyticApiKey(apikey, s)
    s.debug('exists: ' + str(k.exists()))
    r = ResponseData(k)
    today = datetime.date.today()
    r.params(operation = 'select').params(perspective = 'rank')
    r.params(version = 0,
             restrict_begin = (today - datetime.timedelta(weeks = 1)).strftime('%Y-%m-%d'),
             restrict_end = today.strftime('%Y-%m-%d'),
             restrict_kind = 'overview')

    r.sync()
    s.debug('data object: ' + str(r.object))
    s.debug('=============')
    for k in r.object:
        s.debug("\n\tkey: %s\n\tvalue:%s\n-------\n" % (str(k), str(r.object[k])))
        if k == 'rows':
            for ro in r.object[k]:
                s.debug("\n\t\trow(%d): %s\n-------\n" % (len(ro), str(ro)))

if __name__ == '__main__':
    test1()
    test2()
