__author__ = 'badpoet'

import requests
import json

MAX_TRY = 3

class WrongPage(Exception):

    def __str__(self):
        return "Not the correct page."

class WeatherComClient(object):

    def __init__(self):
        self.headers = {
            "Host": "d1.weather.com.cn",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36",
            "Referer": "http://www.weather.com.cn/weather1d/101010100.shtml",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "zh-CN,zh;q=0.8,ru;q=0.6,zh-TW;q=0.4,en;q=0.2"
        }
        self.base_url = "http://d1.weather.com.cn/sk_2d/%s.html"

    def make_url(self, cid):
        return self.base_url % str(cid)

    def make_headers(self, cid):
        return self.headers

    def analyse_html(self, html):
        try:
            return json.loads(html[13:])
        except Exception, e:
            raise WrongPage()

    def fetch_once(self, url, headers):
        resp = requests.get(url, headers=headers, timeout=2)
        if resp.status_code != 200:
            raise WrongPage()
        return self.analyse_html(resp.text)

    def fetch_raw(self, cid):
        return self.fetch_once(self.make_url(cid), self.make_headers(cid))

    def fetch_page(self, cid):
        cnt = 0
        last_exception = None
        while cnt < MAX_TRY:
            try:
                return self.fetch_once(self.make_url(cid), self.make_headers(cid))
            except requests.Timeout, e:
                cnt += 1
                last_exception = e
            except WrongPage, e:
                print "Are we banned?"
                cnt += 1
                last_exception = e
        print last_exception

    def fetch_all(self):
        pass

if __name__ == "__main__":
    wcc = WeatherComClient()
    print wcc.fetch_raw("101010100")