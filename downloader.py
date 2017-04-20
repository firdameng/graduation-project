# -*- coding: utf-8 -*-
import urlparse
import urllib2
import random
import time
import os
import threading
from datetime import datetime, timedelta
import socket

import lxml.html

#from douban_movies_comms import set_query_parameter

DEFAULT_AGENT = 'wswp'
DEFAULT_DELAY = 3
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 60


class Downloader:
    def __init__(self, delay=DEFAULT_DELAY, user_agent=DEFAULT_AGENT, proxies=None, num_retries=DEFAULT_RETRIES,
                 timeout=DEFAULT_TIMEOUT, opener=None, cache=None,cookie=None):
        socket.setdefaulttimeout(timeout)
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.num_retries = num_retries
        self.opener = opener
        self.cache = cache
        self.cookie = cookie

    def __call__(self, url):
        result = None
        if self.cache:
            try:
                result = self.cache[url]
            except KeyError:
                # url is not available in cache 
                pass
            else:
                if self.num_retries > 0 and 500 <= result['code'] < 600:
                    # server error so ignore result from cache and re-download
                    result = None
        if result is None:
            # result was not loaded from cache so still need to download
            self.throttle.wait(url)
            proxy = random.choice(self.proxies) if self.proxies else None
            headers = {'User-agent': self.user_agent}
            if  self.cookie :
                headers['Cookie'] = self.cookie['Cookie']
            result = self.download(url, headers, proxy=proxy, num_retries=self.num_retries)
            if self.cache:
                # save result to cache
                self.cache[url] = result
        return result['html'],result['code']

    def download(self, url, headers, proxy, num_retries, data=None):
        dt0 = time.time()
        # print 'Downloading:\n', url
        print '%s: %s\n' % (threading.currentThread().getName(), urlparse.urlparse(url).query)
        request = urllib2.Request(url, data, headers or {})
        opener = self.opener or urllib2.build_opener()
        if proxy:
            proxy_params = {urlparse.urlparse(url).scheme: proxy}
            opener.add_handler(urllib2.ProxyHandler(proxy_params))
        try:
            dt1 = time.time()  # 测试时间
            # print 'dt1-dt0',dt1-dt0
            response = opener.open(request)
            dt2 = time.time()
            # print 'dt2-dt1',dt2-dt1
            html = response.read()
            code = response.code
        except Exception as e:
            print 'Download error:', str(e)
            html = ''
            if hasattr(e, 'code'):
                code = e.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return self._get(url, headers, proxy, num_retries - 1, data)
            else:
                code = None
        dt3 = time.time()
        print '%s: dt3-dt0 %f' % (threading.currentThread().getName(), dt3 - dt0)  # 抓取一次耗时
        return {'html': html, 'code': code}


class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """

    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urlparse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        # print(last_accessed)
        # print self.delay
        if self.delay > 0 and last_accessed is not None:
            # print self.delay
            # print type(self.delay)
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            # print(last_accessed)
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()

'''
def main():
    base_url = 'https://movie.douban.com/subject/26683290/comments?start=0&limit=20&sort=new_score&status=P'
    cookie = {'Cookie': 'dbcl2=110626009:C37wlSp6zbg'}
    downloader = Downloader(cache=mongo_cache.MongoCache(),cookie=cookie)
    for i in range(0, 100, 20):
        new_url = set_query_parameter(base_url, 'start', i)
        response = downloader(new_url)

        if response['code'] != 200:
            print '访问出错...'
            break

        html = response['html']
        comm_elems = lxml.html.fromstring(html).cssselect('div#comments .comment p')
        comms = []
        for p in comm_elems:
            # print p.text_content()
            comms.append(p.text_content().encode('utf-8'))
        print len(comms)
        with open('comments.txt', 'a+') as f:
            f.writelines(comms)
        print 'finish %d th download' % (i / 20)

if __name__ == '__main__':
    print 'start:%s' % time.ctime()
    s = time.time()
    main()
    print 'end:%s' % time.ctime()
    print  'cost:', time.time()- s
'''