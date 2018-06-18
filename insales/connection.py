# -*- coding: utf-8; -*-

import datetime
import time
from math import log
from urllib.parse import urlparse, parse_qsl, urlunparse, urlencode

from base64 import b64encode
from http.client import HTTPConnection
from collections import defaultdict

class ApiError(Exception):
    def __init__(self, msg, code=None):
        super(ApiError, self).__init__(msg)
        self.code = code


class Connection(object):
    def __init__(self, account, api_key, password):
        self.account = account
        self.api_key = api_key
        self.password = password
        self.sleep = 0.1
        self.limit = (0,500)

    def request(self, method, endpoint, qargs={}, data=None):
        path = self.format_path(endpoint, qargs)
        conn = HTTPConnection('{}.myinsales.ru:80'.format(self.account))
        auth = b64encode(("{}:{}".format(self.api_key, self.password)).encode()).decode()
        headers = {
            'Authorization': 'Basic {}'.format(auth),
            'Content-Type': 'application/xml'
        }
        
        while 1:
            try:
                conn.request(method, path, headers=headers, body=data)
                #print(method, path)
                break
            except Exception as e:
                print('gaierror', e)
                time.sleep(5)
        
        while 1:
            resp = conn.getresponse()
            body = resp.read()
            # Шайтан-машина для экспоненциального роста задержки между запросами
            # Все из расчета на 5 минут = 300 секунд.
            time.sleep(self.sleep)
            self.limit = [int(i) for i in resp.headers['API-Usage-Limit'].split('/')]
            limit_left = max((self.limit[1]-self.limit[0]),1)
            self.sleep = min((300/limit_left)/(log(limit_left,1000)+1)**3+0.05,5)
            
            if 200 <= resp.status < 300:
                return body
            elif resp.status == 503:
                retry_timer = resp.headers['Retry-After']
                print('Connection timed out, retry after {}'.format(retry_timer))
                time.sleep(retry_timer)
            else:
                raise ApiError("{} request to {} returned: {}\n{}".format(method, path, resp.status, body), resp.status)
                break
            
                

    def format_path(self, endpoint, qargs):
        for key, val in qargs.items():
            if isinstance(val, datetime.datetime):
                qargs[key] = val.strftime('%Y-%m-%d+%H:%M:%S')
                
#         url_parts = list(urlparse(endpoint))
#         query = dict(parse_qsl(url_parts[4]))
#         query.update(qargs)
#         url_parts[4] = urlencode(query)

    
        url_parts = list(urlparse(endpoint))
        query = defaultdict(list)
        for k,v in parse_qsl(url_parts[4]):
            query[k].append(v)
        query.update(qargs)
        url_parts[4] = urlencode(query,True)
        
        return urlunparse(url_parts)

    def get(self, path, qargs):
        return self.request('GET', path, qargs=qargs)

    def put(self, path, data):
        return self.request('PUT', path, data=data)

    def post(self, path, data):
        return self.request('POST', path, data=data)

    def delete(self, path):
        return self.request('DELETE', path)