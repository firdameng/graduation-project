# coding=utf-8
import json
import os

from pymongo import MongoClient
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, LongType, ArrayType, StructType, IntegerType, StringType, MapType, FloatType, \
    DoubleType

from adv_table import ADV_CONJ_TABLE, NEG_ADV_TABLE
from pairs_extract import build_re

adv_conj_pattern = build_re(ADV_CONJ_TABLE)

def isAdvConj(w):
    unicodeW = w if isinstance(w, unicode) else w.decode('utf-8')
    return True if adv_conj_pattern.search(unicodeW) else False

neg_adv_pattern = build_re(NEG_ADV_TABLE)

def isNegAdv(w):
    unicodeW = w if isinstance(w, unicode) else w.decode('utf-8')
    return True if neg_adv_pattern.search(unicodeW) else False

def get_sDp_by_sId(sId, cDpResult):
    '''
    根据评论分句的id从评论依存句法结果中获取到该条评论的依存句法分析结果列表
    '''
    sorted(cDpResult,key = lambda x :x['sId'])
    return cDpResult[sId]['sDpResult'] if -1 < sId < len(cDpResult) else None

def del_EMTuple_Lt_Sid(sId, cEvalMulTuples, ):
    '''
    从某条评论的评价多元组中，删除在sid之前的分句评价多元组
    '''
    return filter(lambda y: y['sId'] >= sId, sorted(cEvalMulTuples, key=lambda x: x['sId']))


def get_evalMulTuples(cFeaSen, cDpResult):
    # 发序列化特征情感对集合
    cFeaSenPairs = json.loads(cFeaSen['cFeaSenPairs']) if isinstance(cFeaSen['cFeaSenPairs'],unicode) \
        else cFeaSen['cFeaSenPairs']

    c_eval_mul_tuples = []
    for fea_sen_Pair in cFeaSenPairs:
        s_id = fea_sen_Pair['sId']
        s_FeaSenPairs = fea_sen_Pair['sFeaSenPairs']
        s_dp_result = get_sDp_by_sId(s_id, cDpResult)
        # 首先考虑转折副词的影响，即判断分句中首个词是否是副词,并且是转折副词，
        if s_dp_result and s_dp_result[0]['pos'] in ['c','d']and isAdvConj(s_dp_result[0]['cont']):  # 首先得满足词性
            c_eval_mul_tuples = del_EMTuple_Lt_Sid(s_id, c_eval_mul_tuples)

        # 遍历当前特征情感对所在依存句法结果集
        sEvalMulTuples = []
        for FeaSenPair in s_FeaSenPairs:
            # 过滤不合格特征，都满足了
            # 待寻找的否定词
            deg_adj = {}  #应当初始化为空词
            # 只从该分句的特征词和情感词之间找否定副词
            for w in filter(lambda x: FeaSenPair['feature']['id'] < x['id'] < FeaSenPair['sentiment']['id'],
                            s_dp_result):
                # 如果当前词为情感词的否定副词
                if w['relate'] == 'ADV' and w['parent'] == FeaSenPair['sentiment']['id'] and \
                        isNegAdv(w['cont']):
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

def extract_all_evalMulTuple(database,dp_collection,fea_sen_pairs_collection,eval_mul_tuples_collection):

    def get_dp_result(cId,commDpCollection):
        commdp = commDpCollection.find_one({'cId':cId})
        if commdp:
            if isinstance(commdp['cDpResult'],unicode):
                return json.loads(commdp['cDpResult'])
            else:
                return commdp['cDpResult']
        else:
            return None


    client = MongoClient()
    bufferSize = 100
    try:
        db = client.get_database(database)
        # 获取原始评论的依存关系对
        dp_col = db.get_collection(dp_collection)

        fsp_col = db.get_collection(fea_sen_pairs_collection)
        #写入抽取的评价多元组集合
        emt_col = db.get_collection(eval_mul_tuples_collection)

        bufferEvalMulTuple = []
        cursor = fsp_col.find()
        while True:

            try:
                fsp = cursor.next()
                bufferSize -= 1
                #print bufferSize
                dpResult = get_dp_result(fsp['cId'],dp_col)
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

    app_name = '[APP] bulid_sentimeng_dict'
    master_name = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    #dict_directory = '/home/caiwencheng/Downloads/my_dict/converted'
    dict_directory = os.path.join(os.path.curdir,'sentiment_dict','converted')
    neg_access_path = os.path.join(dict_directory,'neg_assessment_utf8_converted.txt')
    neg_senti_path = os.path.join(dict_directory,'neg_sentiment_utf8_converted.txt')
    neg_pos_path = os.path.join(dict_directory,'NTUSD_negative_simplified_utf8_converted.txt')

    pos_access_path = os.path.join(dict_directory, 'pos_assessment_utf8_converted.txt')
    pos_senti_path = os.path.join(dict_directory, 'pos_sentiment_utf8_converted.txt')
    pos_pos_path = os.path.join(dict_directory, 'NTUSD_positive_simplified_utf8_converted.txt')

    sc = my_spark.sparkContext

    neg_words1 = sc.textFile(neg_access_path).map(lambda y : y.strip()).filter(
        lambda x : x.isspace() == False and isChinese(x)).distinct()
    neg_words1.count()
    neg_words2 = sc.textFile(neg_senti_path).map(lambda y : y.strip()).filter(
        lambda x : x.isspace() == False and isChinese(x)).distinct()
    neg_words2.count()
    neg_words3 = sc.textFile(neg_pos_path).map(lambda y : y.strip()).distinct()
    neg_words3.count()

    certain_neg_words = sc.union([neg_words1,neg_words2,neg_words3]).distinct()
    certain_neg_words.saveAsTextFile(os.path.join(os.path.curdir,'sentiment_dict','neg_dict'))
    certain_neg_words.count()

    pos_words1 = sc.textFile(pos_access_path).map(lambda y : y.strip()).filter(
        lambda x: x.isspace() == False and isChinese(x)).distinct()
    pos_words1.count()
    pos_words2 = sc.textFile(pos_senti_path).map(lambda y : y.strip()).filter(
        lambda x: x.isspace() == False and isChinese(x)).distinct()
    pos_words2.count()
    pos_words3 = sc.textFile(pos_pos_path).distinct()
    pos_words3.count()
    certain_pos_words = sc.union([pos_words1, pos_words2, pos_words3]).distinct()
    certain_pos_words.saveAsTextFile(os.path.join(os.path.curdir,'sentiment_dict', 'pos_dict'))
    certain_pos_words.count()

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
            # if degree_sen == 0 :    #即如果存在未登录情感词，则抛弃
            #     continue
            degree_negadv = 1 if not semtp['negAdv'] else -1
            degValue = degree_sen * degree_negadv
            semtp['degValue'] =  degValue
            sDegrees.append(semtp)
            sDegValue += degValue
        cDegrees.append(
            {
                'sId':semt['sId'],
                #'sDegValue':sDegValue/float(len(sDegrees)),
                'sDegrees':sDegrees
            }
        )
        cDegValue += sDegValue
    return {
        'pId':cfsp['pId'],
        'cId':cfsp['cId'],
        'cDegValue':cDegValue/float(reduce(lambda a,b:a+b,map(lambda x :len(x['sDegrees']),cDegrees))),
        'cDegrees':cDegrees
    }


def calculate_sentiment_degree(spark,database,eval_mul_tuple_collection,comments_degree_collection):
    def dict2row(d):
        d['cDegrees'] = json.dumps(d['cDegrees'])
        return Row(**d)
    sc = spark.sparkContext
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
    emt_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection", eval_mul_tuple_collection).\
        load()
    emt_dict_rdd = emt_df.rdd.map(lambda y : y.asDict(recursive=True))
    emt_dict_rdd.cache()

    # 通过传递广播字典，对评价多元组进行情感计算
    cds_json_rdd = emt_dict_rdd.map(lambda y : getCommentDegreeStatistics(y,broadcast_dict.value))
    cds_json_rdd.cache()

    # print cds_json_rdd.count()
    #
    degreeStatistics = cds_json_rdd.take(1000)
    # # for i in degreeStatistics:
    # #     print i
    for i in degreeStatistics:
        for x in i['cDegrees']:
            for y in x['sDegrees']:
                print i['cId'], i['cDegValue'],x['sId'],\
                    y['feature']['cont'],y['sentiment']['cont'], y['relate'], \
                    y['negAdv']['cont'] if y['negAdv'] else None

    DegreeStatisticsDF = spark.createDataFrame(cds_json_rdd.map(lambda x : dict2row(x)))
    DegreeStatisticsDF.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",comments_degree_collection).\
        save()

def transfrom_comment_degrees(spark, database, comments_degree_collection):
    def deserialize(d):
        d.pop('_id')
        for k, v in d.items():
            if k == 'cDegrees':
                d[k] = json.loads(v)
                for sd in d[k]:
                    for i in sd['sDegrees']:
                        i['negAdv'] = {} if not i['negAdv'] else i['negAdv']
            else:
                d[k] = v

        return d

    degrees_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  comments_degree_collection).\
        load()

    degrees_rdd = degrees_df.rdd.map(lambda y: y.asDict(recursive=True)). \
        map(lambda x: deserialize(x))

    fields = [
        StructField('pId', LongType(), False),
        StructField('cId', LongType(), False),
        StructField('cDegValue', DoubleType(), False),
        StructField('cDegrees',
                    ArrayType(
                        StructType([
                            StructField('sId', IntegerType(), True),
                            StructField('sDegrees', ArrayType(
                                StructType([
                                    StructField('feature', MapType(StringType(), StringType(), True), True),
                                    StructField('sentiment', MapType(StringType(), StringType(), True), True),
                                    StructField('negAdv', MapType(StringType(), StringType(), True), True),
                                    StructField('relate', StringType(), False),
                                    StructField('degValue', IntegerType(), True)
                                ]), True), True)
                        ]), True), True)
    ]

    schema = StructType(fields)
    temp = spark.createDataFrame(degrees_rdd, schema)
    temp.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  'temp3'). \
        save()

if __name__ == '__main__':
    product_id = 4297772  #4297772
    database = 'jd'
    dp_collection = 'comments_dp_%d'%product_id
    fea_sen_pairs_collection= 'fea_sen_pairs_%d'%product_id
    eval_mul_tuples_collection = 'eval_mul_tuples_%d'%product_id
    #extract_all_evalMulTuple(database,dp_collection,fea_sen_pairs_collection,eval_mul_tuples_collection)
    #bulid_sentimeng_dict()

    app_name = '[APP] bulid_sentimeng_dict'
    master_name = 'spark://caiwencheng-K53BE:7077'

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    comments_degree_collection = 'comments_degree_%d'%product_id
    #calculate_sentiment_degree(spark, database, eval_mul_tuples_collection, comments_degree_collection)
    #
    # product_summary_collection = 'product_summary_%d'%product_id
    # purged_comments_collection = 'purged_comments'
    # summary_comment_classfication(spark,database,comments_degree_collection,purged_comments_collection,
    #                               product_summary_collection)
    transfrom_comment_degrees(spark,database,comments_degree_collection)  #有bug
    pass