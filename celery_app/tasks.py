# coding=utf-8

# celery_app 有了__init__.py这个文件就成了模块了
import json
import urllib

from celery_app import app
from celery.result import AsyncResult
import xml.etree.ElementTree as ET

url_get_base = "http://127.0.0.1:12345/ltp"
args = {
    'x': 'n',
    't': 'dp'
}


@app.task
def error_handler(uuid):
    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
        uuid, exc, result.traceback))


@app.task(bind=True, default_retry_delay=60, max_retries=10)
def dp_comment(self, comment):
    # 数据库评论是unicode啊,先utf-8编码
    args['s'] = comment['content'].encode('utf-8')
    # for k, v in args.items():
    #     args[k] = v.encode('utf-8')
    try:
        xml_str = urllib.urlopen(url_get_base, urllib.urlencode(args)).read()  # POST method
        if not xml_str or xml_str == '':
            raise Exception('[response null] text: %s' % args['s'])
        # 关联评论ID和依存句法分析对
        cDpResult = []
        for s in ET.fromstring(xml_str).iter('sent'):
            cDpResult.append(
                {
                    'sId': int(s.attrib['id']),
                    'sDpResult': map(lambda w: w.attrib, s.findall('word'))
                }
            )
        # 正常pid应该为comment['productId']，celery当字符串存储到mongodb去了
        return {
            'pId': 3133811,
            'cId': comment['_id'],
            'cDpResult': cDpResult
        }
        # return  xml_str
    except Exception as exc:
        print args['s']
        raise self.retry(exc=exc)  # 遵从默认重连参数


@app.task(bind=True, default_retry_delay=60, max_retries=10)
def ltp_comment_np_xml(self, comment):
    args['s'] = comment['content']
    utf8_args = {}
    for k, v in args.items():
        utf8_args[k] = v.encode('utf-8')
    try:
        xml_str = urllib.urlopen(url_get_base, urllib.urlencode(utf8_args)).read()  # POST method
        if not xml_str or xml_str == '':
            raise Exception('[response null] text: %s' % utf8_args['s'])

        # 关联评论ID和依存句法分析对
        jResult = {comment['_id']: []}
        for s in ET.fromstring(xml_str).iter('sent'):
            jResult[comment['_id']].append(map(lambda w: w.attrib, s.findall('word')))
        return jResult
        # return  xml_str
    except Exception as exc:
        print utf8_args['s']
        raise self.retry(exc=exc)  # 遵从默认重连参数


# 利用.task 标记add函数为celery的一个任务
# defalut_retry_delay 指的是在1分钟后重新任务，次数10
@app.task(bind=True, default_retry_delay=60, max_retries=10)
def ltp_comment_np_json(self, text):
    args['x'] = text
    utf8_args = {}
    for k, v in args.items():
        utf8_args[k] = v.encode('utf-8')
    try:
        json_str = urllib.urlopen(url_get_base, urllib.urlencode(utf8_args)).read()  # POST method
        if not json_str or json_str == '':
            raise Exception('[ null ] text: %s' % utf8_args['text'])
        np_result = json.loads(json_str)[0][0]
        return np_result
    except Exception as exc:
        print exc.message
        raise self.retry(exc=exc)  # 遵从默认重连参数

        # timeout = Timeout(3)
        # try:
        #     rInt = randint(2,6)
        #     time.sleep(rInt)
        #     result = {'rid':rInt,'content':'the %d th download...'%rInt}
        #     return result
        # except Timeout as t:
        #     if t is not timeout:
        #         raise #not my timeout
        #     print 'timeout,retrying....'
        # finally:
        #     timeout.cancel()
