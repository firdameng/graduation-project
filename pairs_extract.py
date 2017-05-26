# coding=utf-8
# 此模块是特征情感对抽取模块
import json
import re

from pymongo import MongoClient
from pyspark import Row
from pyspark.sql import SparkSession

import feed_features
from celery_app import tasks


def dp_comments(database, product_id, purged_comments_collection):
    client = MongoClient()
    # 动态改变celery backend
    # print celery_app.app.conf.CELERY_MONGODB_BACKEND_SETTINGS
    # #celery_app.app.conf.CELERY_MONGODB_BACKEND_SETTINGS['taskmeta_collection'] = 'comments_dp_%d' % product_id
    #
    # celery_app.app.conf.update(
    #     CELERY_MONGODB_BACKEND_SETTINGS  = {
    #         'database': 'jd',
    #         'taskmeta_collection':'comments_dp_%d' % product_id
    #     }
    # )
    # print celery_app.app.conf.CELERY_MONGODB_BACKEND_SETTINGS
    try:
        collection = client.get_database(database).get_collection(purged_comments_collection)
        cursor = collection.find().limit(100)
        i = 0
        for c in cursor:
            i += 1
            print i
            tasks.dp_comment.delay(c)
    finally:
        client.close()


def format_dp_comments(spark, database, dp_collection, formatted_dp_collection):
    def dict2row(d):
        d['cDpResult'] = json.dumps(d['cDpResult'])
        return Row(**d)

    dp_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", dp_collection). \
        load()

    formatted_dp_rdd = dp_df.rdd.map(lambda y: y.asDict(recursive=True)). \
        filter(lambda x: x['status'] == 'SUCCESS').\
        map(lambda x: json.loads(x['result']))

    formatted_dp_df = spark.createDataFrame(formatted_dp_rdd.map(lambda x: dict2row(x)))
    formatted_dp_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", formatted_dp_collection). \
        save()

def extract_comments_dp_pairs(spark, database, formatted_dp_collection,dp_paris_collection):
    # wId 假定在 commDp范围内
    def get_word_from_comm(wId, sDpResult):
        for w in sDpResult:
            if w['id'] == wId:
                return w

    def getCDpPairs(c_dp):
        '''
        解析依存句法评论句的最小单元，分句为单位，返回四种依存句法对sbv..
        cDpResult 是个包含多个分句
        '''
        cDpResult = json.loads(c_dp['cDpResult']) if isinstance(c_dp['cDpResult'],unicode) else c_dp['cDpResult']
        cDpPairs = []
        for sDp in cDpResult:

            sDpPairs = []
            for w in sDp['sDpResult']:
                if w['relate'] in ['SBV', 'VOB', 'ATT', 'CMP']:
                    '''首先添加满足这四类关系的两个词'''
                    w_relate = w['relate']
                    w_child = get_word_from_comm(w['parent'], sDp['sDpResult'])
                    sDpPairs.append(
                        {
                            'first': w if w_relate in ['ATT', 'SBV'] else w_child,
                            'second': w_child if w_relate in ['ATT', 'SBV'] else w,
                            'wRelate': w_relate
                        }
                    )
                    m_child = m_parent = None
                    for m in sDp['sDpResult']:
                        # 如果m和w并列，则添加(w_parent,m)
                        if m['relate'] == 'COO' and m['parent'] == w['id']:
                            m_parent = m
                            sDpPairs.append(
                                {
                                    'first': m if w_relate in ['ATT', 'SBV'] else w_child,
                                    'second': w_child if w_relate in ['ATT', 'SBV'] else m,
                                    'wRelate': w_relate
                                })
                        # 如果m和w_parent并列，则添加(m,w)
                        if m['relate'] == 'COO' and m['parent'] == w['parent']:
                            m_child = m
                            sDpPairs.append(
                                {
                                    'first': w if w_relate in ['ATT', 'SBV'] else m,
                                    'second': m if w_relate in ['ATT', 'SBV'] else w,
                                    'wRelate': w_relate
                                })
                    if m_parent and m_child:
                        sDpPairs.append(
                            {
                                'first': m_parent if w_relate in ['ATT', 'SBV'] else m_child,
                                'second': m_child if w_relate in ['ATT', 'SBV'] else m_parent,
                                'wRelate': w['relate']
                            })
            cDpPairs.append(
                {
                    'sId': sDp['sId'],
                    'sDpPairs': sDpPairs
                }
            )
        return {
            'pId': c_dp['pId'],
            'cId': c_dp['cId'],
            'cDpPairs': cDpPairs
        }

    def dict2row(d):
        d['cDpPairs'] = json.dumps(d['cDpPairs'])
        return Row(**d)

    formatted_dp_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", formatted_dp_collection). \
        load()

    # formatted_dp_rdd = dp_df.rdd.map(lambda y: y.asDict(recursive=True)). \
    #     map(lambda x: json.loads(x['result']))

    formatted_dp_rdd = formatted_dp_df.toJSON().map(lambda x: json.loads(x))

    dp_pairs_rdd = formatted_dp_rdd.map(lambda x : getCDpPairs(x))

    test = dp_pairs_rdd.take(20)
    for i in test:
        for sdp in i['cDpPairs']:
            for sDpPair in sdp['sDpPairs']:
                print i['cId'],sdp['sId'],sDpPair['first']['cont'],sDpPair['second']['cont'],sDpPair['wRelate']

    dp_pairs_df = spark.createDataFrame(dp_pairs_rdd.map(lambda x: dict2row(x)))
    dp_pairs_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", dp_paris_collection). \
        save()

feature_word_dict = {}
temp_feature_word_dict = {}
temp_sentiment_word_dict = {}
# feed_sentiment_words = {}

def extract_sentiment(dpPairStr, senti_judge_fuction, *args):
    global feature_word_dict
    global temp_feature_word_dict
    global temp_sentiment_word_dict

    dpPair = json.loads(dpPairStr) if isinstance(dpPairStr, unicode) else dpPairStr
    relate = dpPair['wRelate']
    feature = dpPair['second'] if relate == 'ATT' else dpPair['first']
    featureWord = feature['cont'] if isinstance(feature['cont'],unicode) else feature['cont'].decode('utf-8')
    if len(feature['cont']) < 2 or len(feature['cont']) > 4:
        return 1
    if senti_judge_fuction(featureWord):  # featureWord长度小于2的特征不考虑
        if args[0] == 0: #直接写入特征词集合
            feature_word_dict[featureWord] = feature_word_dict[featureWord] + 1 if \
                featureWord in feature_word_dict else 1
        else:
            temp_feature_word_dict[featureWord] += 1
        sentiment = dpPair['first'] if relate == 'ATT' else dpPair['second']
        sentimentWord = sentiment['cont'] if isinstance(sentiment['cont'],unicode) else sentiment['cont'].decode('utf-8')
        if len(sentimentWord) < 1 or len(sentimentWord) > 3:
            return 1
        if sentiment['pos'] == 'a':
            temp_sentiment_word_dict[sentimentWord] = temp_sentiment_word_dict[sentimentWord] + 1 if \
                sentimentWord in temp_sentiment_word_dict else 1
            return -1
        return 1
    return 0


def extract_feature(dpPairStr):
    global temp_feature_word_dict
    global temp_sentiment_word_dict

    dpPair = json.loads(dpPairStr) if isinstance(dpPairStr, unicode) else dpPairStr
    relate = dpPair['wRelate']
    sentiment = dpPair['first'] if relate == 'ATT' else dpPair['second']

    sentimentWord = sentiment['cont'] if isinstance(sentiment['cont'],unicode) else sentiment['cont'].decode('utf-8')
    # 情感词长度小于1或大于3 直接标记为非特征情感对
    if len(sentimentWord) <1 or len(sentimentWord) > 3 :
        return 1
    ''' unicode中文 只可以比对相同编码的中文dict'''
    # 满足情感词长度要求但不在候选特征情感集合中，标记为待定 0
    if  sentimentWord in temp_sentiment_word_dict:
        temp_sentiment_word_dict[sentimentWord] += 1
        feature = dpPair['second'] if relate == 'ATT' else dpPair['first']
        featureWord = feature['cont'] if isinstance(feature['cont'],unicode) else feature['cont'].decode('utf-8')
        # 特征词不满足要求直接标记删除
        if len(feature['cont']) < 2 or len(feature['cont']) > 4 :
            return 1
        # 特征词不满足要求，但情感词已判定，则也标记为删除
        if feature['pos'] in ['n', 'v']:
            temp_feature_word_dict[featureWord] = temp_feature_word_dict[featureWord] + 1 if \
                featureWord in temp_feature_word_dict else 1
            return -1
        return 1
    return 0

def add_deleted_field(comm_dps_str):
    comment_dps = json.loads(comm_dps_str) if isinstance(comm_dps_str, unicode) else comm_dps_str
    comment_dps['cDpPairs'] = json.loads(comment_dps['cDpPairs']) if isinstance(comment_dps['cDpPairs'],unicode) else \
        comment_dps['cDpPairs']
    for s_dp in comment_dps['cDpPairs']:
        for s_dp_pair in s_dp['sDpPairs']:
            s_dp_pair['deleted'] = 0  # 1 删除,0 非特征情感对，-1特征情感对
    return comment_dps


def iter_dataset(dataset, function, *arg):
    '''
    :param dataset: 
    :param function: 有三种情况下的函数
    :param arg: 
    :return: 
    '''
    for comm_dp_pair in dataset:
        for s_dp in comm_dp_pair['cDpPairs']:
            for s_dp_pair in s_dp['sDpPairs']:
                if s_dp_pair['deleted'] == 0: # 只迭代待定的依存对
                    s_dp_pair['deleted'] = function(s_dp_pair, *arg)
    return dataset

def build_re(pattern_str):
    pattern = '(%s)' % '|'.join(pattern_str)
    return re.compile(pattern, re.UNICODE)


RE = build_re(feed_features.features)

def extract_fea_sen_pairs(spark,database,dp_pairs_collection,fea_sen_pairs_collection):
    global feature_word_dict
    global temp_feature_word_dict
    global temp_sentiment_word_dict
    # global feed_sentiment_words
    def inFeedFeatureSet(featureWord):
        unicode_feature_word = featureWord if isinstance(featureWord, unicode) else featureWord.decode('utf-8')
        return False if not RE.search(unicode_feature_word) else True

    def inTempFeatureWordSet(featureWord):
        return True if featureWord in temp_feature_word_dict else False

    # def inFeedSentimentSet(sentimentWord):
    #     return True if sentimentWord in feed_sentiment_words else False
    #
    # def inTempSentimentWordSet(sentimentWord):
    #     return True if sentimentWord in temp_sentiment_word_dict else False

    def include_fsp(c_temp_fea_sen_result):
        # 过滤掉不包含特征情感对的评论
        for s_dp in c_temp_fea_sen_result['cDpPairs']:
            for s_dp_pair in s_dp['sDpPairs']:
                if s_dp_pair['deleted'] == -1:
                    return True
        return False

    def filter_fea_sen_pair(c_temp_fea_sen_result):
        '''
        :param c_temp_fea_sen_result: 前提是临时特征情感集中有特征情感对，提取评论分句中的特征情感对
        '''
        c_fea_sen_pairs = []
        for s_dp in c_temp_fea_sen_result['cDpPairs']:
            s_fea_sen_pairs = []
            for s_dp_pair in s_dp['sDpPairs']:
                if s_dp_pair['deleted'] == -1:
                    s_fea_sen_pairs.append(
                        {
                            'feature': s_dp_pair['first'] if s_dp_pair['wRelate'] != 'ATT' else s_dp_pair['second'],
                            'sentiment': s_dp_pair['second'] if s_dp_pair['wRelate'] != 'ATT' else s_dp_pair['first'],
                            'relate': s_dp_pair['wRelate']
                        }
                    )
            if s_fea_sen_pairs:   #过滤掉了那种空特征情感分句
                c_fea_sen_pairs.append(
                    {
                        'sId': s_dp['sId'],
                        'sFeaSenPairs': s_fea_sen_pairs
                    }
                )
        return {
                'pId': c_temp_fea_sen_result['pId'],
                'cId': c_temp_fea_sen_result['cId'],
                'cFeaSenPairs': c_fea_sen_pairs
            }

    def purge_fea_sen_pair(temp_fea_sen_result):
        # 除去不合规格非特征词，情感词
        for s_dp in temp_fea_sen_result['cDpPairs']:
            for s_dp_pair in s_dp['sDpPairs']:
                unicode_feature_word = s_dp_pair['feature']['cont'] if isinstance(s_dp_pair['feature']['cont'],unicode) \
                    else s_dp_pair['feature']['cont'].decode('utf-8')
                unicode_sentiment_word = s_dp_pair['sentiment']['cont'] if isinstance(s_dp_pair['sentiment']['cont'],
                                                                                      unicode)\
                                    else s_dp_pair['sentiment']['cont'].decode('utf-8')
                if 1 <= len(unicode_sentiment_word) <= 3 and 2 <= len(unicode_feature_word) <= 4:
                    return True
        return False

    def dict2row(d):
        '''
        复杂的结构化会出现 sId = None
        '''
        d['cFeaSenPairs'] = json.dumps(d['cFeaSenPairs'])
        return Row(**d)

    # 加载4中依存句法对评论集
    dp_pairs_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", dp_pairs_collection). \
        load()

    # 构造情感种子词典  很难归纳出种子词典啊

    # 添加删除域为迭代做铺垫
    marked_dp_pairs = dp_pairs_df.toJSON().map(lambda x: add_deleted_field(x)).collect()

    # 用list迭代,内存还是够的
    marked_dp_pairs = iter_dataset(marked_dp_pairs, extract_sentiment, inFeedFeatureSet, 0)

    # 统计候选特征和情感词总个数
    old_len =( 0 if not temp_feature_word_dict else reduce(lambda x,y:x+y, temp_feature_word_dict.values()))  +\
             0 if not temp_sentiment_word_dict else reduce(lambda x, y: x + y, temp_sentiment_word_dict.values())
    print '候选特征词数：{0}，候选情感词数：{1}' \
        .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict))

    while True:
        marked_dp_pairs = iter_dataset(marked_dp_pairs, extract_feature)
        print '候选特征数：{0}，候选情感词数：{1}' \
            .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict))
        marked_dp_pairs = iter_dataset(marked_dp_pairs, extract_sentiment, inTempFeatureWordSet, 1)
        print '候选特征数：{0}，候选情感词数：{1}' \
            .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict))
        cur_len = reduce(lambda x,y:x+y, temp_feature_word_dict.values()) + \
                  reduce(lambda x,y:x+y, temp_sentiment_word_dict.values())
        print ('old_len:%d,cur_len:%d') % (old_len, cur_len)

        # 如果候选特征和情感词个数及其数量不发生改变，则迭代完成
        if cur_len != old_len:
            old_len = cur_len
        else:
            break

    # 提取带特征情感词的依存句法对，并写入fea_sen_pairs_collection          map(lambda y:purge_fea_sen_pair(y)).\
    marked_dp_pairs_rdd = spark.sparkContext.parallelize(marked_dp_pairs)
    row_fsp_rdd = marked_dp_pairs_rdd.filter(lambda x: include_fsp(x)).\
        map(lambda x: filter_fea_sen_pair(x)). \
        map(lambda z: dict2row(z))
    fea_sen_pairs_df = spark.createDataFrame(row_fsp_rdd)
    fea_sen_pairs_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").\
        option("uri", "mongodb://127.0.0.1/").option("database",database).option("collection",fea_sen_pairs_collection).\
        save()

    # 记录候选特征词集和候选情感词集 dict ->rdd,阀值3
    threshold_value = 3
    feature_word_dict.update(dict(filter(lambda t :  t[1]> threshold_value,temp_feature_word_dict.items())))
    sentiment_word_dict =dict(filter(lambda t : t[1] > threshold_value,temp_sentiment_word_dict.items()))

    feature_word_dict_rdd = spark.sparkContext.parallelize(feature_word_dict.items())
    sentiment_word_dict_rdd = spark.sparkContext.parallelize(sentiment_word_dict.items())
    spark.createDataFrame(feature_word_dict_rdd.map(lambda x:Row(name =x[0],freq=x[1]))).write.format("com.mongodb.spark.sql.DefaultSource").\
        mode("overwrite").option("uri", "mongodb://127.0.0.1/").option("database",database).\
        option("collection",'feature_word_%d'%product_id).save()
    spark.createDataFrame(sentiment_word_dict_rdd.map(lambda x: Row(name =x[0],freq=x[1]))).write.format(
        "com.mongodb.spark.sql.DefaultSource"). \
        mode("overwrite").option("uri", "mongodb://127.0.0.1/").option("database", database). \
        option("collection", 'sentiment_word_%d' % product_id).save()

if __name__ == '__main__':
    product_id = 3899582    #4297772
    database = 'jd'
    purged_comments_collection = 'purged_comments_%d'%product_id
    temp_dp_collection = 'temp_comments_dp_%d' % product_id

    # ---------------修改celery的bakend值---------------------
    dp_comments(database, product_id, purged_comments_collection)

    # app_name = 'caiwencheng'
    # master_name = 'spark://caiwencheng-K53BE:7077'
    # my_spark = SparkSession \
    #     .builder \
    #     .appName(app_name) \
    #     .master(master_name) \
    #     .getOrCreate()
    # comments_dp_collection = 'comments_dp_%d'%product_id
    # dp_pairs_collection = 'comments_dp_pairs_%d'%product_id
    # fea_sen_pairs_collection = 'fea_sen_pairs_%d'%product_id
    #format_dp_comments(my_spark,database,temp_dp_collection,comments_dp_collection)
    #extract_comments_dp_pairs(my_spark,database,comments_dp_collection,dp_pairs_collection)
    #extract_fea_sen_pairs(my_spark,database,dp_pairs_collection,fea_sen_pairs_collection)