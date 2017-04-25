# coding=utf-8
import json
import urllib

if __name__ == '__main__':
    url_get_base = "http://api.ltp-cloud.com/analysis/"
    args = {
        'api_key' : 'q171J1g398dtDdMxpzHDsKJgFh9WRIXHfK5hOo8H',
        'text' : '手机的外壳真漂亮。',
        'pattern' : 'dp',
        'format' : 'json'
    }
    result = urllib.urlopen(url_get_base, urllib.urlencode(args)) # POST method
    content = json.load(result)[0][0]
    print content