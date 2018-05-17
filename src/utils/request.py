#!/bin/python3
#-*- coding: UTF-8 -*-
import requests


header = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'm.mtime.cn',
    'Pragma': 'no-cache',
    'Referer': 'http://m.mtime.cn/',
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1',
    'X-Mtime-Wap-CheckValue': 'mtime',
    'X-Requested-With': 'XMLHttpRequest'
}

def get_proxy():
    return str(requests.get("http://localhost:5010/get/", timeout=1000).content, encoding="utf-8")


def delete_proxy(proxy):
    requests.get("http://localhost:5010/delete/?proxy={}".format(proxy))

def request(method, url, **kwargs):
    retry_count = 5
    change_proxy = 5
    while change_proxy >0:
        # proxy = get_proxy()
        while retry_count > 0:
            try:
                kwargs = {**kwargs,**{
                    # 'proxies': {"http": "http://{}".format(proxy)},
                    'headers': header,
                    "timeout":2
                }}
                html = requests.request(method,url,**kwargs)
                # 使用代理访问
                return html
            except Exception:
                retry_count -= 1
        # delete_proxy(proxy)
        change_proxy -= 1
    return None

def get(url, params=None, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    return request('get', url, params=params, **kwargs)


def options(url, **kwargs):
    kwargs.setdefault('allow_redirects', True)
    return request('options', url, **kwargs)


def head(url, **kwargs):
    kwargs.setdefault('allow_redirects', False)
    return request('head', url, **kwargs)


def post(url, data=None, json=None, **kwargs):
    return request('post', url, data=data, json=json, **kwargs)


def put(url, data=None, **kwargs):
    return request('put', url, data=data, **kwargs)


def patch(url, data=None, **kwargs):
    return request('patch', url,  data=data, **kwargs)


def delete(url, **kwargs):
    return request('delete', url, **kwargs)


# print(get("http://m.mtime.cn/Service/callback.mi/movie/Detail.api?movieId=253823&locationId=290&t=201851515431174283").json())
