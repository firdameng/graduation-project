# coding=utf-8
import json
import os
import re
from time import ctime, time

import jieba
from pymongo import MongoClient
from pyspark.sql import SparkSession, Row

from celery_app import tasks, celeryconfig


def isValidSimilarity(comm):
    '''
    统计分词的set数和list数，相似度大于0.5判定无效
    :param comm: 
    :return: 
    
    s.translate(None, string.punctuation)
    strip(string.punctuation)
    '''
    seg_generator = jieba.cut(comm, cut_all=False)
    seg_list = '/'.join(seg_generator).split('/')
    return False if (1.0 - len(set(seg_list)) / float(len(seg_list))) > 0.5 else True


def isValidElement(comm):
    '''
    元素只为字母，数字和标点符号判定无效
    :param comm: 
    :return: 
    '''
    # returncps = u'。？！，、；：（）""【】[]-——～·《》<>.'
    # ps='^[%s]*$'%cps
    # pat=re.compile(ps,re.UNICODE)
    # pat.match(u'？！，、；')
    # pat.match(u'？哈哈、；')
    # pat.match(u'哈哈刚刚')
    # patternStr='^(\w|%s)*$' % '|'.join(u'。？！，、；：（）""【】[]-——～·《》<>.')
    # pattern = re.compile(patternStr,re.UNICODE)
    # r = pattern.match(comm)
    return True


def isIncludeUrl(comm):
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', comm)
    return False if not urls else True


def isValidLength(comm):
    unicodeComm = comm if isinstance(comm, unicode) else comm.decode('utf-8')
    return True if len(unicodeComm) >= 4 and len(unicodeComm) <= 100 else False


def preprocessComment(comm):
    if not isValidSimilarity(comm):
        return False
    # if not isValidElement(comm):
    #     return False
    if isIncludeUrl(comm):
        return False
    if not isValidLength(comm):
        return False
    return True


def parseAllComment():
    db_name = 'jd'
    coll_name = 'productId_3133811'
    client = MongoClient()
    db = client.get_database(db_name)
    collection = db.get_collection(coll_name)
    cursor = collection.find()
    i = 0
    for c in cursor:
        if preprocessComment(c['content']):
            i += 1
            tasks.dp_comment.delay(c)
    print i
    client.close()


# wId 假定在 commDp范围内
def getWordFromComm(wId, sDpResult):
    for w in sDpResult:
        if w['id'] == wId:
            return w


def getCDpPairs(cDpResult):
    '''
    解析依存句法评论句的最小单元，分句为单位，返回四种依存句法对sbv..
    cDpResult 是个包含多个分句
    '''
    CDpPairs = []
    for sDp in cDpResult:

        sDpPairs = []
        for w in sDp['sDpResult']:
            if w['relate'] in ['SBV', 'VOB', 'ATT', 'CMP']:
                '''首先添加满足这四类关系的两个词'''
                w_relate = w['relate']
                w_child = getWordFromComm(w['parent'], sDp['sDpResult'])
                sDpPairs.append(
                    {
                        'parent': w if w_relate in ['ATT', 'SBV'] else w_child,
                        'child': w_child if w_relate in ['ATT', 'SBV'] else w,
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
                                'parent': m if w_relate in ['ATT', 'SBV'] else w_child,
                                'child': w_child if w_relate in ['ATT', 'SBV'] else w,
                                'wRelate': w_relate
                            })
                    # 如果m和w_parent并列，则添加(m,w)
                    if m['relate'] == 'COO' and m['parent'] == w['parent']:
                        m_child = m
                        sDpPairs.append(
                            {
                                'parent': w if w_relate in ['ATT', 'SBV'] else m,
                                'child': m if w_relate in ['ATT', 'SBV'] else w,
                                'wRelate': w_relate
                            })
                if m_parent and m_child:
                    sDpPairs.append(
                        {
                            'parent': m_parent if w_relate in ['ATT', 'SBV'] else m_child,
                            'child': m_child if w_relate in ['ATT', 'SBV'] else m_parent,
                            'wRelate': w['relate']
                        })
        CDpPairs.append(
            {
                'sId': sDp['sId'],
                'sDpPairs': sDpPairs
            }
        )
    return CDpPairs


def parseCommentDp(commentDpStr):
    commentDp = json.loads(commentDpStr)
    cDpPairs = getCDpPairs(commentDp['cDpResult'])
    # print cDpPairs
    return {
        'pId': commentDp['pId'],
        'cId': commentDp['cId'],
        'cDpPairs': cDpPairs
    }


def parseAllCommentDp():
    dp_db_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['database']
    dp_coll_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['taskmeta_collection']
    client = MongoClient()
    commentDpPairs = client.get_database('jd').get_collection('commentDpPairs_3133811')
    try:
        db = client.get_database(dp_db_name)
        collection = db.get_collection(dp_coll_name)
        cursor = collection.find({'status': 'SUCCESS'})
        count = 0
        for c in cursor:
            # temp = parseCommentDp(c['result'])
            # for sdp in temp['cDpPairs']:
            #     for sDpPair in sdp['sDpPairs']:
            #         print temp['cId'],sDpPair['parent']['cont'],sDpPair['child']['cont'],sDpPair['wRelate']
            commentDpPairs.insert_one(parseCommentDp(c['result']))
            count += 1
            print count
    finally:
        client.close()


feedFeatures = [
    # 主体
    u'品牌', u'型号',

    # 基本信息
    u'机身', u'颜色', u'长度', u'宽度', u'厚度', u'重量', u'材质',

    # 系统
    u'系统', u'版本',

    # 芯片
    u'芯片', u'cpu', u'品牌', u'频率', u'核数', u'型号',

    # 网络支持
    u'SIM', u'网络', u'G', u'移动', u'联通', u'电信',

    # 存储
    u'存储', u'ROM', u'RAM', u'GB',

    # 屏幕
    u'屏幕', u'尺寸', u'分辨率',

    # 摄像头
    u'摄像头', u'数量', u'光圈', u'像素',

    # 电池
    u'电池', u'容量', u'拆卸', u'充电器', u'mA',

    # 数据接口
    u'数据', u'USB', u'接口', u'红外', u'WIFI', u'NFC', u'蓝牙', u'热点', u'耳机', u'充电',

    # 手机特性
    u'GPS', u'指纹', u'陀螺仪',

    # 包装清单
    u'包装', u'售后', u'服务', u'手机', u'取卡针', u'保护'
]


def build_re(pattern_str):
    RE = '(%s)' % '|'.join(pattern_str)
    return re.compile(RE, re.UNICODE)


RE = build_re(feedFeatures)


# 中文正则表达式这里是必须用unicode编码
def inFeedFeatureSet(featureWord):
    stafeatureWord = featureWord if isinstance(featureWord, unicode) else featureWord.decode('utf-8')
    result = False if not RE.search(stafeatureWord) else True
    return result


def inTempFeatureWordSet(featureWord):
    result = True if featureWord in TempFeatureWordSet else False
    return result


FeatureWordSet = list()  # 最终的特征词集合
SentimentWordSet = list()  # 最终的情感词集合
FeaSenPairSet = list()  # 最终的特征情感对集合

TempFeatureWordSet = dict()
TempSentimentWordSet = dict()


def extract_sentiment(dpPairStr, senti_judge_fuction, *args):
    global FeatureWordSet
    global TempFeatureWordSet
    global TempSentimentWordSet
    global FeaSenPairSet

    dpPair = json.loads(dpPairStr) if isinstance(dpPairStr, unicode) else dpPairStr
    relate = dpPair['wRelate']
    feature = dpPair['child'] if relate == 'ATT' else dpPair['parent']
    featureWord = feature['cont']
    if len(featureWord) > 1 and senti_judge_fuction(featureWord):  # featureWord长度小于2的特征不考虑
        if args[0] == 0:
            # FeatureWordSet.append(featureWord)
            # 并非完全匹配，应当加入候选特征集
            TempFeatureWordSet[featureWord] = TempFeatureWordSet[featureWord] + 1 if \
                featureWord in TempFeatureWordSet else 1
        else:
            TempFeatureWordSet[featureWord] += 1
        print 'len(TempFeatureWordSet)', len(TempFeatureWordSet)
        sentiment = dpPair['parent'] if relate == 'ATT' else dpPair['child']
        if sentiment['pos'] == 'a':
            sentimentWord = sentiment['cont']
            TempSentimentWordSet[sentimentWord] = TempSentimentWordSet[sentimentWord] + 1 if \
                sentimentWord in TempSentimentWordSet else 1

            print 'len(TempSentimentWordSet)', len(TempSentimentWordSet)
            FeaSenPairSet.append(
                {
                    'feature': feature,
                    'sentiment': sentiment,
                    'relate': dpPair['wRelate']
                }
            )

            print len(FeaSenPairSet)
            print featureWord, sentimentWord
            return 1
        return -1
    return 0


def extract_feature(dpPairStr):
    global TempFeatureWordSet
    global TempSentimentWordSet
    global FeaSenPairSet

    dpPair = json.loads(dpPairStr) if isinstance(dpPairStr, unicode) else dpPairStr
    relate = dpPair['wRelate']
    sentiment = dpPair['parent'] if relate == 'ATT' else dpPair['child']

    sentimentWord = sentiment['cont']
    ''' unicode中文 只可以比对相同编码的中文dict'''
    if sentimentWord in TempSentimentWordSet:

        TempSentimentWordSet[sentimentWord] += 1
        print 'len(TempSentimentWordSet)', len(TempSentimentWordSet)
        feature = dpPair['child'] if relate == 'ATT' else dpPair['parent']
        if feature['pos'] in ['n', 'v']:
            featureWord = feature['cont']
            TempFeatureWordSet[featureWord] = TempFeatureWordSet[featureWord] + 1 if \
                featureWord in TempFeatureWordSet else 1
            print 'len(TempFeatureWordSet)', len(TempFeatureWordSet)
            FeaSenPairSet.append(
                {
                    'feature': feature,
                    'sentiment': sentiment,
                    'relate': dpPair['wRelate']
                }
            )
            print len(FeaSenPairSet)
            print featureWord, sentimentWord
            return 1

        return -1
    return 0


def fetch_all_dp_pairs(commentDpPair):
    comment_dp_pair = json.loads(commentDpPair) if isinstance(commentDpPair, unicode) else commentDpPair
    s_dp_pairs = []
    for s_dp in comment_dp_pair['cDpPairs']:
        for s_dp_pair in s_dp['sDpPairs']:
            s_dp_pair['sId'] = s_dp['sId']
            s_dp_pair['cId'] = comment_dp_pair['cId']
            s_dp_pairs.append(s_dp_pair)
    return s_dp_pairs


def add_deleted_field(comm_dps_str):
    comment_dps = json.loads(comm_dps_str) if isinstance(comm_dps_str, unicode) else comm_dps_str
    for s_dp in comment_dps['cDpPairs']:
        for s_dp_pair in s_dp['sDpPairs']:
            s_dp_pair['deleted'] = 0  # -1 删除,0 非特征情感对，1特征情感对
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
                if s_dp_pair['deleted'] > -1:
                    s_dp_pair['deleted'] = function(s_dp_pair, *arg)
    return dataset


def include_fsp(c_temp_fea_sen_result):
    for s_dp in c_temp_fea_sen_result['cDpPairs']:
        for s_dp_pair in s_dp['sDpPairs']:
            if s_dp_pair['deleted'] == 1:
                return True
    return False


def dict2row(d):
    '''
    复杂的结构化会出现 sId = None
    :param d: 
    :return: 
    '''
    d['cFeaSenPairs'] = json.dumps(d['cFeaSenPairs'])
    return Row(**d)


def purge_fea_sen_pair(c_temp_fea_sen_result):
    '''

    :param c_temp_fea_sen_result: 前提是临时特征情感集中有特征情感对
    :return: 
    '''
    c_fea_sen_pairs = []
    for s_dp in c_temp_fea_sen_result['cDpPairs']:
        s_fea_sen_pairs = []
        for s_dp_pair in s_dp['sDpPairs']:
            if s_dp_pair['deleted'] == 1:
                s_fea_sen_pairs.append(
                    {
                        'feature': s_dp_pair['parent'] if s_dp_pair['wRelate'] != 'ATT' else s_dp_pair['child'],
                        'sentiment': s_dp_pair['child'] if s_dp_pair['wRelate'] != 'ATT' else s_dp_pair['parent'],
                        'relate': s_dp_pair['wRelate']
                    }
                )
        # 有依存关系对，但不存在特征情感对时，至少保留一个分句的结构
        # if not s_fea_sen_pairs:
        #     s_fea_sen_pairs.append(
        #         {
        #             'feature':None,
        #             'sentiment':None,
        #             'relate':None
        #         }
        #     )
        # 保证只装了有特征情感对的分句，rdd->df  0-》none->null
        if s_fea_sen_pairs:
            c_fea_sen_pairs.append(
                {
                    'sId': s_dp['sId'],
                    'sFeaSenPairs': s_fea_sen_pairs
                }
            )
    # if not c_fea_sen_pairs:
    #     c_fea_sen_pairs.append(
    #         {
    #             'sId':-1,
    #             'sFeaSenPairs':{
    #                 'feature': None,
    #                 'sentiment': None,
    #                 'relate': None
    #             }
    #         }
    #     )
    temp = {
        'pId': c_temp_fea_sen_result['pId'],
        'cId': c_temp_fea_sen_result['cId'],
        'cFeaSenPairs': c_fea_sen_pairs
    }
    # print temp
    return temp


def parse_all_comm_dp_pair():
    app_name = '[APP] parse_comment_dp_pair'
    master_name = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.commentDpPairs_3133811") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/jd.commentFeaSenPairs_3133811") \
        .master(master_name) \
        .getOrCreate()


    df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    marked_comm_dp = df.toJSON().map(lambda x: add_deleted_field(x)).collect()
    dataset = iter_dataset(marked_comm_dp, extract_sentiment, inFeedFeatureSet, 0)
    old_len = len(TempFeatureWordSet) + len(TempSentimentWordSet)
    print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}' \
        .format(len(TempFeatureWordSet), len(TempSentimentWordSet), len(FeaSenPairSet))
    while True:
        dataset = iter_dataset(dataset, extract_feature)
        print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}' \
            .format(len(TempFeatureWordSet), len(TempSentimentWordSet), len(FeaSenPairSet))
        dataset = iter_dataset(dataset, extract_sentiment, inTempFeatureWordSet, 1)
        print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}' \
            .format(len(TempFeatureWordSet), len(TempSentimentWordSet), len(FeaSenPairSet))
        cur_len = len(TempFeatureWordSet) + len(TempSentimentWordSet)
        print ('precount:%d,currcount:%d') % (old_len, cur_len)
        if cur_len != old_len:
            old_len = cur_len
        else:
            break

    fsp_rdd = my_spark.sparkContext.parallelize(dataset)
    row_fsp_rdd = fsp_rdd.filter(lambda x: include_fsp(x)).map(lambda x: purge_fea_sen_pair(x)). \
        map(lambda x: dict2row(x))
    FeaSenPairSetDF = my_spark.createDataFrame(row_fsp_rdd)
    FeaSenPairSetDF.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    my_spark.stop()


    # -------------------------
    #fsp_rdd.distinct()
    # -------------------------


def isInWordTable(w, pattern):
    unicodeW = w if isinstance(w, unicode) else w.decode('utf-8')
    return True if pattern.search(unicodeW) else False


# 否定副词表
ADV_CONJ_TABLE = [u'不', u'没']

adv_conj_pattern = build_re(ADV_CONJ_TABLE)


def isAdvConj(w):
    return isInWordTable(w, adv_conj_pattern)


# 转折副词表
NEG_ADV_TABLE = [u'但是', u'然而', u'只是', u'可是', u'不过', u'但', u'不料']

neg_adv_pattern = build_re(NEG_ADV_TABLE)


def isNegAdv(w):
    return isInWordTable(w, neg_adv_pattern)


def get_sDp_by_sId(sId, cDpResult):
    '''
    根据评论分句的id从评论依存句法结果中获取到该条评论的依存句法分析结果列表
    :param sId: 
    :param cDpResult: 
    :return: 
    '''
    sorted(cDpResult,key = lambda x :x['sId'])
    return cDpResult[sId]['sDpResult'] if -1 < sId < len(cDpResult) else None


def del_EMTuple_Lt_Sid(sId, cEvalMulTuples, ):
    '''
    从某条评论的评价多元组中，删除在sid之前的分句评价多元组
    :param sId: 
    :param cEvalMulTuples: 
    :return: 
    '''
    return filter(lambda y: y['sId'] >= sId, sorted(cEvalMulTuples, key=lambda x: x['sId']))


def get_evalMulTuples(cFeaSen, cDpResult):


    # def getCDpResult(c_id,c_dp_df):
    #     print '-----------getcDpResult---------------------'
    #     print c_id
    #     comm_celery_result = c_dp_df.filter(c_dp_df['cId'] == c_id).collect()
    #     print comm_celery_result
    #     return comm_celery_result[0].asDict(recursive=True)['cDpResult'] if comm_celery_result else None

    # print '-----------get_evalMulTuples---------------------'
    # print cFeaSen
    # print cDpResult

    # 发序列化特征情感对集合
    cFeaSenPairs = json.loads(cFeaSen['cFeaSenPairs'])

    # 获取当前评论的依存句法集合
    #cDpResult = getCDpResult(cFeaSen['cId'],cDpDf)
    # print type(cFeaSenPairs)

    c_eval_mul_tuples = []
    for fea_sen_Pair in cFeaSenPairs:
        s_id = fea_sen_Pair['sId']
        s_FeaSenPairs = fea_sen_Pair['sFeaSenPairs']
        s_dp_result = get_sDp_by_sId(s_id, cDpResult)

        # 首先考虑转折副词的影响，即判断分句中首个词是否是副词,并且是转折副词，
        if s_dp_result and s_dp_result[0]['pos'] in ['c','d']and isNegAdv(s_dp_result[0]['cont']):  # 首先得满足词性
            c_eval_mul_tuples = del_EMTuple_Lt_Sid(s_id, c_eval_mul_tuples)

        # 遍历当前特征情感对所在依存句法结果集
        sEvalMulTuples = []
        for FeaSenPair in s_FeaSenPairs:
            # 过滤不合格特征
            if len(FeaSenPair['feature']['cont']) < 2:
                continue
            # 待寻找的否定词
            deg_adj = None
            # 只从该分句的特征词和情感词之间找否定副词
            for w in filter(lambda x: FeaSenPair['feature']['id'] < x['id'] < FeaSenPair['sentiment']['id'],
                            s_dp_result):
                # 如果当前词为情感词的否定副词
                if w['relate'] == 'ADV' and w['parent'] == FeaSenPair['sentiment']['id'] and \
                        isAdvConj(w['cont']):
                    # 判断当前不否定词为None，则将w付给deg_adj，否则双重否定置为None
                    deg_adj = w if not deg_adj else None
            FeaSenPair['negAdv'] = deg_adj
            sEvalMulTuples.append(FeaSenPair)
        c_eval_mul_tuples.append(
            {
                'sId': s_id,
                'sEvalMulTuples': sEvalMulTuples
            }
        )
    return {
        'pId': cFeaSen['pId'],
        'cId': cFeaSen['cId'],
        'cEvalMulTuples': c_eval_mul_tuples
    }

def extract_all_evalMulTuple1():

    def get_dp_result(cId,commDpCollection):
        commdp = commDpCollection.find_one({'cId':cId})
        return commdp['cDpResult'] if commdp else None

    client = MongoClient()
    bufferSize = 100
    try:

        # 获取原始评论的依存关系对
        sta_dp_col = client.get_database('jd').get_collection('standard_commentDps_3133811')

        fsp_col = client.get_database('jd').get_collection('commentFeaSenPairs_3133811')
        #写入抽取的评价多元组集合
        emt_col = client.get_database('jd').get_collection('commentEvalMulTuples_3133811')

        bufferEvalMulTuple = []
        cursor = fsp_col.find()
        while True:

            try:
                fsp = cursor.next()
                bufferSize -= 1
                print bufferSize
                dpResult = get_dp_result(fsp['cId'],sta_dp_col)
                evalMulTuple = get_evalMulTuples(fsp,dpResult)

                for x in evalMulTuple['cEvalMulTuples']:
                        for y in x['sEvalMulTuples']:
                            if y['negAdv']:
                                print evalMulTuple['cId'],x['sId'],y['feature']['cont'],\
                                    y['sentiment']['cont'],y['negAdv']['cont'],y['relate']

            except Exception as e:
                # 遍历完了也要先写入数据库再退出
                print e.message
                if bufferEvalMulTuple:
                    emt_col.insert_many(bufferEvalMulTuple)
                print '遍历结束'
                break
            bufferEvalMulTuple.append(evalMulTuple)
            if bufferSize == 0:
                print '开始插入'
                emt_col.insert_many(bufferEvalMulTuple)
                bufferSize = 100  # 重设大小
                bufferEvalMulTuple = []
    finally:
        client.close()

def extract_all_evalMulTuple():

    # 根据当前特征情感对所在评论id获取 当前评论的依存句法对
    def get_dp_result(cId,commDpCollection):
        cursor = commDpCollection.find_one({'cId':cId})
        return cursor.next()['cDpResult'] if cursor else None

    app_name = '[APP] extract_evalMulTuple'
    master_name = 'spark://caiwencheng-K53BE:7077'
    client = MongoClient()
    sta_col = client.get_database('jd').get_collection('standard_commentDps_3133811')

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/jd.commentEvalMulTuples_3133811") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.commentFeaSenPairs_3133811") \
        .master(master_name) \
        .getOrCreate()

    # 本身就是字典型
    df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    # 延迟加载而已,sql查询不了非结构化吗？filter+反序列化，还不如client直接查询，本身存的就是字符串啊尼玛
    # dp_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
    #             "mongodb://127.0.0.1/jd.standard_commentDps_3133811").load()
    df_dict_rdd = df.limit(10).rdd.map(lambda y : y.asDict(recursive=True))
    t = df_dict_rdd.map(lambda x: get_evalMulTuples(x,get_dp_result(x['cId'],sta_col)))

    for i in t.collect():
        for x in i['cEvalMulTuples']:
            for y in x['sEvalMulTuples']:
                print i['cId'],x['sId'],y['feature']['cont'],y['sentiment']['cont'],y['negAdv']['cont'],y['relate']

def standard_commmentdp():
    client = MongoClient()
    col = client.get_database('jd').get_collection('commentDps_3133811')
    sta_col = client.get_database('jd').get_collection('standard_commentDps_3133811')
    cursor = col.find()
    for c in cursor:
        sta_col.insert_one(json.loads(c['result']))
    client.close()

def bulid_sentimeng_dict():

    def isChinese(s):
        if not s:
            return False
        s = s.strip()
        unicode_s = s if isinstance(s, unicode) else s.decode('utf-8')
        for c in unicode_s:
            if c < u'\u4e00' or c > u'\u9fff':
                return False
        return True

    # def isChinese(s):
    #     print type(s)
    #     print s.encode('utf-8') if isinstance(s,unicode) else s
    #
    #     # 移除前后留白
    #     s = s.strip()
    #     if not s:
    #         return False
    #
    #     unicode_s = s if isinstance(s,unicode) else s.decode('utf-8')
    #
    #     print type(unicode_s)
    #
    #     for c in unicode_s:
    #         print type(c)
    #         if c < u'\u4e00' or c > u'\u9fff':
    #             print c.encode('utf-8') if isinstance(c, unicode) else c
    #             return False
    #     return True

    def findSimilarWords(w,kvSimilarDictRdd,SimilarDictRdd):
        simi_words = [w]
        simi_keys = kvSimilarDictRdd.lookup(w)
        for k in simi_keys:
            simi_words += SimilarDictRdd.lookup(k)[0]
        return list(set(simi_words))

    app_name = '[APP] bulid_sentimeng_dict'
    master_name = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    dict_directory = '/home/caiwencheng/Downloads/my_dict/converted'
    neg_access_path = os.path.join(dict_directory,'neg_assessment_utf8_converted.txt')
    neg_senti_path = os.path.join(dict_directory,'neg_sentiment_utf8_converted.txt')
    neg_pos_path = os.path.join(dict_directory,'NTUSD_negative_simplified_utf8_converted.txt')

    pos_access_path = os.path.join(dict_directory, 'pos_assessment_utf8_converted.txt')
    pos_senti_path = os.path.join(dict_directory, 'pos_sentiment_utf8_converted.txt')
    pos_pos_path = os.path.join(dict_directory, 'NTUSD_positive_simplified_utf8_converted.txt')

    sc = my_spark.sparkContext

    neg_words1 = sc.textFile(neg_access_path).map(lambda y : y.strip()).filter(
        lambda x : x.isspace() == False and isChinese(x)).distinct()
    neg_words2 = sc.textFile(neg_senti_path).map(lambda y : y.strip()).filter(
        lambda x : x.isspace() == False and isChinese(x)).distinct()
    neg_words3 = sc.textFile(neg_pos_path).map(lambda y : y.strip()).distinct()

    certain_neg_words = sc.union([neg_words1,neg_words2,neg_words3]).distinct()
    certain_neg_words.saveAsTextFile(os.path.join(os.path.curdir,'sentiment_dict','neg_dict'))
    certain_neg_words.count()

    pos_words1 = sc.textFile(pos_access_path).map(lambda y : y.strip()).filter(
        lambda x: x.isspace() == False and isChinese(x)).distinct()
    pos_words2 = sc.textFile(pos_senti_path).map(lambda y : y.strip()).filter(
        lambda x: x.isspace() == False and isChinese(x)).distinct()
    pos_words3 = sc.textFile(pos_pos_path).distinct()

    certain_pos_words = sc.union([pos_words1, pos_words2, pos_words3]).distinct()
    certain_pos_words.saveAsTextFile(os.path.join(os.path.curdir,'sentiment_dict', 'pos_dict'))
    certain_pos_words.count()

    # # 只找同义词的那一行
    # similar_words = sc.textFile(similar_word_path).map(lambda x : (x.split(' ')[0] ,x.split(' ')[1:]))\
    #     .filter(lambda y : True if y[0].find(u'=') > -1 else False)
    # print 'similar_words'
    # similar_words.count()
    #
    # kv_similar_words = similar_words.flatMapValues(lambda x : x).map(lambda y : (y[1] ,y[0]))
    # print 'kv_similar_words'
    # kv_similar_words.count()
    #
    # new_neg_words = list()
    # for w in  certain_neg_words.toLocalIterator():
    #     new_neg_words.append(w)
    #     new_neg_words += findSimilarWords(w,kv_similar_words,similar_words)
    #     len(new_neg_words)
    #
    # distinctNegRdd = sc.parallelize(new_neg_words).flatMap(lambda x : x).distinct()
    # print 'distinctNegRdd',distinctNegRdd.count()
    # for w in distinctNegRdd.limit(100).collect():
    #     print w

    #filter(lambda x : isChinese(x),sc.textFile(neg_access_path).take(20))

def assess_word_degree(w,dicts):

    if not w:
        return 0
    w = w if isinstance(w,unicode) else w.decode('utf-8')
    kvNegDict = dicts['kvNegDict']
    kvPosDict = dicts['kvPosDict']
    kvSimiDict = dicts['kvSimiDict']
    simiDict = dicts['simiDict']
    if w in kvNegDict:      # lookup返回的是一个list
        return -1
    if w in kvPosDict:
        return 1
    if w in kvSimiDict:
        real_key = kvSimiDict[w]
        for v in simiDict[real_key]:
            if v in kvNegDict:
                return -1
            if v in kvPosDict:
                return 1
    return 0


def getCommentDegreeStatistics(cfsp,dicts):
    cDegValue = 0
    cDegrees = []
    for semt in cfsp['cEvalMulTuples']:
        sDegrees = []
        sDegValue = 0
        for semtp in semt['sEvalMulTuples']:
            degree_sen = assess_word_degree(semtp['sentiment']['cont'],dicts)  #表明接受的可变参数列表的第一个
            degree_negadv = 1 if not semtp['negAdv'] else -1
            degValue = degree_sen * degree_negadv
            semtp['degValue'] =  degValue
            sDegrees.append(semtp)
            sDegValue += degValue
        cDegrees.append(
            {
                'sId':semt['sId'],
                'sDegValue':sDegValue,
                'sDegrees':sDegrees
            }
        )
        cDegValue += sDegValue
    return {
        'pId':cfsp['pId'],
        'cId':cfsp['cId'],
        'cDegValue':cDegValue,
        'cDegrees':cDegrees
    }


def calculate_sentiment_degree():
    def dict2row(d):
        d['cDegrees'] = json.dumps(d['cDegrees'])
        return Row(**d)
    ''''''
    app_name = '[APP] calculate_sentiment_degree'
    master_name = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.commentEvalMulTuples_3133811") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/jd.test_commentDegreeStatistics_3133811") \
        .master(master_name) \
        .getOrCreate()
    sc = my_spark.sparkContext
    #/home/caiwencheng/PycharmProjects/graduation-project
    # 构造词典kvRdd
    dictdir = os.path.join(os.path.curdir,'sentiment_dict')

    kv_neg_dict = sc.textFile(os.path.join(dictdir,'neg_dict')).map(lambda x : (x,True)).collectAsMap()

    kv_pos_dict = sc.textFile(os.path.join(dictdir, 'pos_dict')).map(lambda x: (x, True)).collectAsMap()

    simirdd = sc.textFile(os.path.join(dictdir, 'similar_dict_utf8_converted.txt'))\
        .map(lambda x : (x.split(' ')[0] ,x.split(' ')[1:]))\
        .filter(lambda y : True if y[0].find(u'=') > -1 else False)
    simirdd.cache()

    simi_dict = simirdd.collectAsMap()
    kv_simi_dict = simirdd.flatMapValues(lambda x : x).map(lambda y : (y[1] ,y[0])).collectAsMap()


    # 构造广播变量，字典（包括kv_neg_rdd，kv_pos_rdd，simirdd，kv_simirdd）
    broadcast_value = {
        'kvNegDict':kv_neg_dict,
        'kvPosDict':kv_pos_dict,
        'simiDict':simi_dict,
        'kvSimiDict':kv_simi_dict
    }
    broadcast_dict = sc.broadcast(broadcast_value)

    # 加载评价多元组数据
    emt_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    emt_dict_rdd = emt_df.rdd.map(lambda y : y.asDict(recursive=True))
    emt_dict_rdd.cache()

    # 通过传递广播字典，对评价多元组进行情感计算
    cds_json_rdd = emt_dict_rdd.map(lambda y : getCommentDegreeStatistics(y,broadcast_dict.value))
    cds_json_rdd.cache()

    # print cds_json_rdd.count()
    #
    # degreeStatistics = cds_json_rdd.collect()
    # # for i in degreeStatistics:
    # #     print i
    # for i in degreeStatistics:
    #     for x in i['cDegrees']:
    #         for y in x['sDegrees']:
    #             print i['cId'], i['cDegValue'],x['sId'],\
    #                 y['feature']['cont'],y['sentiment']['cont'], y['relate'], \
    #                 y['negAdv']['cont'] if y['negAdv'] else None

    DegreeStatisticsDF = my_spark.createDataFrame(cds_json_rdd.map(lambda x : dict2row(x)))
    DegreeStatisticsDF.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    my_spark.stop()


def summary_comment_classfication():
    ''''''
    app_name = '[APP] summary_comment_classfication'
    master_name = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.test_commentDegreeStatistics_3133811") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/jd.productSummaryStatistics_3133811") \
        .master(master_name) \
        .getOrCreate()
    cds_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    #剔除评论情感值为0的数据，保留极性值大于0或小于0的
    valid_cds_rdd = cds_df.rdd.map(lambda y : y.asDict(recursive=True))\
        .filter(lambda x : x['cDegValue'] != 0)\
        .map(lambda z : (z['cId'],1 if z['cDegValue'] > 0 else -1))\
        .sortBy(lambda x: x[0])
    valid_cds_rdd.cache()

    # 建立有效情感评论集的k,v字典，作为广播变量用于过滤原始评论
    valid_cds_map = valid_cds_rdd.collectAsMap()
    broadcast_cds_map = my_spark.sparkContext.broadcast(valid_cds_map)

    # 加载原始评论，根据有效情感评论集，得到和有效情感评论相同的原始精简k,v评论集
    ori_comm_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
        "uri","mongodb://127.0.0.1/jd.productId_3133811").load()
    ori_valid_comm_rdd = ori_comm_df.rdd.map(lambda y : y.asDict(recursive=True)).\
        filter(lambda x : x['_id'] in broadcast_cds_map.value ).\
        map(lambda z : (z['_id'],1 if z['score'] > 3 else -1)).\
        sortBy(lambda x: x[0])
    ori_valid_comm_rdd.cache()
    # 计算精确率(正向而言)
    tp = ori_valid_comm_rdd.map(lambda x:x[1]).\
        zip(valid_cds_rdd.map(lambda y:y[1])).\
        filter(lambda z : z[0] == z[1] and z[0] > 0).\
        count()
    classified_true = valid_cds_rdd.filter(lambda x : x[1] > 0).count()
    actually_true = ori_valid_comm_rdd.filter(lambda x : x[1] > 0).count()

    print '精确率 P',tp/float(classified_true)
    print '召回率 R',tp/float(actually_true)

if __name__ == '__main__':
    #parse_all_comm_dp_pair()
   # extract_all_evalMulTuple1()

    #bulid_sentimeng_dict()
    # print 'start:%s' % ctime()
    # s = time()
    # calculate_sentiment_degree()
    # print 'end:%s' % ctime()
    # print  'cost:', time() - s
    summary_comment_classfication()
    pass