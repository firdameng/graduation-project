# coding=utf-8

# 该模块用于产品评论信息采集
import json

import jieba
import re
from pyspark import Row

from celery_app import tasks


def crawl_pruduct_comments(id, cback, maxsize):
    '''
    
    :param id:   #  如 4297772 诺基亚6   3133811 iphone7lus
    :param cback: @ 如fetchJSON_comment98vv49564,fetchJSON_comment98vv75201(对应上)
    :return: 
    '''
    base_url = 'http://club.jd.com/comment/skuProductPageComments.action?' \
               'callback=@&productId=#&score=0&sortType=5&' \
               'page=$&pageSize=10&isShadowSku=0'.replace('@', cback).replace('#', str(id))
    for i in range(maxsize / 10):
        tasks.download_comments.delay(base_url.replace('$', str(i)))


def format_comments(spark, product_id, database, raw_collection, formatted_collection):
    ''' 从粗处理的评论数据中获取格式化的评论数据并存储起来'''
    raw_data_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", raw_collection). \
        load()

    data_value_rdd = raw_data_df.rdd.map(lambda y: y.asDict(recursive=True)). \
        filter(lambda z: z['status'] == 'SUCCESS' and not z['result'] == False). \
        map(lambda x: (x['_id'], json.loads(x['result']))). \
        flatMapValues(lambda v: v). \
        values(). \
        repartition(2)  # 保证k,v分区一致

    datacount = data_value_rdd.count()

    data_key_rdd = spark.sparkContext.parallelize(range(0, datacount)).repartition(2)
    formatted_data_rdd = data_key_rdd.zip(data_value_rdd). \
        map(lambda x: {'pId': product_id, 'cId': x[0], 'content': x[1]['content'], 'score': x[1]['score']})

    formatted_data_df = spark.createDataFrame(formatted_data_rdd.map(lambda x: Row(**x)))
    formatted_data_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", formatted_collection). \
        save()


def pre_process_comments(spark, database, formatted_collection,purged_collection):
    def valid_similarity(comm):
        unicodeComm = comm if isinstance(comm, unicode) else comm.decode('utf-8')
        seg_generator = jieba.cut(unicodeComm, cut_all=False)
        seg_list = '/'.join(seg_generator).split('/')
        return False if (1.0 - len(set(seg_list)) / float(len(seg_list))) > 0.5 else True

    #元素只为字母，数字和标点符号判定无效
    def valid_element(comm):
        return True

    def include_url(comm):
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', comm)
        return False if not urls else True

    def valid_length(comm):
        unicodeComm = comm if isinstance(comm, unicode) else comm.decode('utf-8')
        return True if len(unicodeComm) >= 4 and len(unicodeComm) <= 100 else False

    noise_comments_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", formatted_collection). \
        load()

    purged_comments_rdd = noise_comments_df.rdd.map(lambda y: y.asDict(recursive=True)). \
        filter(lambda x: valid_element(x['content']) and valid_similarity(x['content'])
                         and valid_length(x['content']) and not include_url(x['content']))

    purged_comments_df = spark.createDataFrame(purged_comments_rdd.map(lambda x: Row(**x)))
    purged_comments_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", purged_collection). \
        save()
