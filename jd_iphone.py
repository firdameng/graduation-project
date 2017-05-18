# coding=utf-8
import ast
import threading
from time import ctime, time, sleep
from urllib import urlencode
from urlparse import parse_qs, urlsplit, urlunsplit

from pymongo import ReturnDocument, MongoClient

import mongo_cache
from downloader import Downloader
from mongo_helper import Mongodb_Helper

DATABASE_NAME = 'jd'
PAGE_SIZE = 10
PAGE_NAME = "page"
DATA_FIELDS = ['content','score']


# !!!! 前提是新建了counter集合，并插入_id = collectionName
def getNextSequence(name, db):
    #print db.name
    #print db.counters.name
    ret = db.counters.find_one_and_update(
        {'_id': name},
        {'$inc': {'seq': 1}},
        return_document=ReturnDocument.AFTER,
        upsert=True)
    return ret['seq']   #ret是一个字典对象


# 负责写回数据，如果需要写回才创建数据库或者数据库
def write2DB(data, client, collectionName):
    db = client.get_database(DATABASE_NAME)  # 存在就返回，不存在就创建
    collection = db.get_collection(collectionName)  # 同理，没有就新建

    # 如果记录只有一条，包装成公用迭代代码
    if not isinstance(data,list):
        data = list(data)
    for item in data:
        # 自增插入
        document = {'_id':getNextSequence(collectionName,db)}
        for field in DATA_FIELDS:
             # 要求data的一条记录item的key 和 field对应
            document[field] = item[field]
        collection.insert_one(document)

# 从response.read()中得到想要的格式化数据
def parseResponse(data):
    # 获取原始一页评论集信息 [comment0,comment1,...commentn],其中comment是字典类型
    dictStr = data[data.find('(') + 1:-2]
    astDictStr = dictStr.replace("true", "True").replace("false", "False").replace("null", "None")
    dictResult = ast.literal_eval(astDictStr)
    oriComments = dictResult["comments"]

    # 精处理我们所需信息，提取 content,score
    comments = []
    for com in oriComments:
        comment = {}
        comment["content"] = com["content"].decode("GBK").encode("utf-8")
        comment["score"] = com["score"]
        comments.append(comment)
    return comments


def set_query_parameter(url, param_name, param_value):
    #print param_name,param_value
    scheme, netloc, path, query_string, fragment = urlsplit(url)
    query_params = parse_qs(query_string)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return urlunsplit((scheme, netloc, path, new_query_string, fragment))


def task(base_url, start, stop, client,collectionName):
    print start,stop
    downloader = Downloader(cache=mongo_cache.MongoCache())
    for i in range(start/PAGE_SIZE, stop/PAGE_SIZE):
        new_url = base_url.replace('$',str(i))      # 替换page值即可
        #print new_url
        data =''
        stime = 30
        while data == '' or not data:
            data, code = downloader(new_url)
            if  data:
                comments = parseResponse(data)
                write2DB(comments,client,collectionName)
                print 'finish %d th download' % (i +1)
                break
            # 目前存在爬去过快，返回空的情况，休息3秒再爬
            print 'data:{0},page:{1},stime:{2}'.format(data,i,stime)
            stime = stime + 5
            sleep(stime)


def main():
    # base_url = 'http://club.jd.com/comment/skuProductPageComments.action?' \
    #            'callback=fetchJSON_comment98vv49564&productId=4297772&score=0&sortType=5&' \
    #            'page=0&pageSize=10&isShadowSku=0'
    # jd_col_name = base_url.split('&')[1].replace('=','_')

    # 构建下载url
    product_id = 4297772
    cback = 'fetchJSON_comment98vv49564'

    base_url = 'http://club.jd.com/comment/skuProductPageComments.action?' \
               'callback=@&productId=#&score=0&sortType=5&' \
               'page=$&pageSize=10&isShadowSku=0'.replace('@',cback).replace('#',str(product_id))

    # 分配多线程任务
    maxsize = 20000
    thread_sum = 20
    comments_one_thread = maxsize / thread_sum
    threads = []

    # 创建或者获取jd集合
    jd_client = MongoClient()
    jd_db = jd_client.get_database(DATABASE_NAME)

    jd_col_name = 'product_'+str(product_id)
    # 创建或这获取counter集合,并初始化商品集合的计数为0
    jd_col_counters = jd_db.get_collection('counters')
    jd_col_counters.insert_one(
        {
            '_id':jd_col_name,
            'seq':0
        }
    )
    #db_douban_helper = Mongodb_Helper(db=DATABASE_NAME)
    #comm_collection = db_douban_helper.create_collection('movie_' + movie_id)

    for t in range(thread_sum):
        start = t * comments_one_thread
        stop = (t + 1) * comments_one_thread
        thread = threading.Thread(target=task, args=(base_url, start, stop,jd_client,jd_col_name))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    #jd_client.close()


if __name__ == '__main__':
    print 'start:%s' % ctime()
    s = time()
    main()
    #url = 'http://club.jd.com/comment/skuProductPageComments.action?sortType=5&pageSize=10&callback=fetchJSON_comment98vv75201&score=0&isShadowSku=0&page=777&productId=3133811'
    #downloader = Downloader(cache=mongo_cache.MongoCache())
    #data, code = downloader(url)
    print 'end:%s' % ctime()
    print  'cost:', time() - s
