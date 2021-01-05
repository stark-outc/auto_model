# coding:utf-8


from config import TIME_SLEEP, TIME_OUT, HOST

import time
import requests

def post(url, headers=None, data=None, json=None):
    time.sleep(TIME_SLEEP)
    try:
        if headers == None:
            headers = get_headers()
        r = requests.post(url, headers=headers, data=data, json=json, timeout=TIME_OUT)
    except:
        r = None
    return r

def put(url, headers=None, data=None):
    time.sleep(TIME_SLEEP)
    try:
        if headers == None:
            headers = get_headers()
        r = requests.put(url, headers=headers, data=data, timeout=TIME_OUT)
    except:
        r = None
    return r

def get(url, headers=None):
    time.sleep(TIME_SLEEP)
    try:
        if headers == None:
            headers = get_headers()
        r = requests.get(url, headers=headers, timeout=TIME_OUT)
    except:
        r = None
    return r



def get_headers():
    return {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    'Host': '%s:8080'%HOST,
    'Origin': 'http://%s:8080'%HOST,
    'Referer': 'http://%s:8080/'%HOST,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    }
