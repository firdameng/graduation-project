# coding=utf-8
import json
import re

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

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


def build_re():
    RE = '(%s)' % '|'.join(feedFeatures)
    #print RE
    return re.compile(RE, re.UNICODE)


RE = build_re()


# 中文正则表达式这里是必须用unicode编码
def inFeedFeatureSet(featureWord):

    stafeatureWord = featureWord if isinstance(featureWord, unicode) else featureWord.decode('utf-8')
    result = False if not RE.search(stafeatureWord) else True
    #print '种子特征判断',stafeatureWord,result
    return result




FeatureWordSet = list()  # 最终的特征词集合
SentimentWordSet = list()  # 最终的情感词集合
FeaSenPairSet = list()  # 最终的特征情感对集合

temp_feature_word_dict = dict()
temp_sentiment_word_dict = dict()


def inTempFeatureWordSet(featureWord):
    #stafeatureWord = featureWord if isinstance(featureWord, unicode) else featureWord.decode('utf-8')
    result =True if featureWord in temp_feature_word_dict else False
    #print '候选特征判断',featureWord,result
    return result

def extractSentiment(dpPairStr, senti_judge_fuction, *args):
    global FeatureWordSet
    global temp_feature_word_dict
    global temp_sentiment_word_dict
    global FeaSenPairSet

    # 加载四种依存关系对为json串
    dpPair = json.loads(dpPairStr)
    # 获取当前依存对的特征-情感词关系
    relate = dpPair['w_relate']
    # 提取关系对中的特征，ATT关系的即child 为特征词,utf-8编码还是个谜
    feature = dpPair['child'] if relate == 'ATT' else dpPair['parent']

    featureWord = feature['cont'].encode('utf-8')
    # 首先判别特征是否在种子特征集或者在候选特征集
    if senti_judge_fuction(featureWord):

        # 若是在迭代执行，args=1,加入候选特征集，否则直接加入特征词集
        if args[0] == 0:
            #FeatureWordSet.append(featureWord)
            # 并非完全匹配，应当加入候选特征集
            TempFeatureWordSet[featureWord] = TempFeatureWordSet[featureWord] + 1 if \
                featureWord in TempFeatureWordSet else 1

        else:
            TempFeatureWordSet[featureWord] += 1
        #print 'len(TempFeatureWordSet)', len(TempFeatureWordSet)
        # 提取当前关系对中情感词，ATT关系的即为parent
        sentiment = dpPair['parent'] if relate == 'ATT' else dpPair['child']

        # 若提取的情感词词性为形容词，先判断是否已在候选特征集，再计数
        if sentiment['pos'] == 'a':
            sentimentWord = sentiment['cont'].encode('utf-8')
            TempSentimentWordSet[sentimentWord] = TempSentimentWordSet[sentimentWord] + 1 if \
                sentimentWord in TempSentimentWordSet else 1

            #print 'len(TempSentimentWordSet)', len(TempSentimentWordSet)
            # 同时满足在候选特征集和候选情感词集的关系对加入特征-情感词集
            FeaSenPairSet.append(
                {
                    'c_id': dpPair['s_id'],
                    's_id': dpPair['id'],
                    'feature_word': feature,
                    'sentiment_word': sentiment,
                    'relate':dpPair['w_relate']
                }
            )
            #print len(FeaSenPairSet)
            #print dpPair['s_id'], featureWord, sentimentWord
        return False
    return True


def extractFeature(dpPairStr):
    global temp_feature_word_dict
    global temp_sentiment_word_dict
    global FeaSenPairSet

    # 加载四种依存关系对为json串
    dpPair = json.loads(dpPairStr)
    # 获取当前依存对的特征-情感词关系
    relate = dpPair['w_relate']
    # 提取关系对中的情感，ATT关系的即parent为特征词,utf-8编码还是个谜
    sentiment = dpPair['parent']if relate == 'ATT' else dpPair['child']

    sentimentWord = sentiment['cont'].encode('utf-8')
    # utf-8比对dict没问题，unicode比对就不知合不合适了，实际上key存储的格式是utf-8
    #print '候选情感判断',sentimentWord,True if sentimentWord in TempSentimentWordSet else False
    # 首先判别情感是否在候选情感集
    if sentimentWord in TempSentimentWordSet:
        # 如果在的话，直接计数加1
        TempSentimentWordSet[sentimentWord] += 1
        #print 'len(TempSentimentWordSet)', len(TempSentimentWordSet)
        # 在满足候选情感集的情况下，提取当前关系对中的特征词
        feature = dpPair['child'] if relate == 'ATT' else dpPair['parent']
        # 判断特征词词性是否为名词或动词
        if feature['pos'] in ['n', 'v']:

            # 如果满足词性，在加入候选特征词集前检查是否已存在该集合中，再进行计数
            featureWord = feature['cont'].encode('utf-8')
            TempFeatureWordSet[featureWord] = TempFeatureWordSet[featureWord] + 1 if \
                featureWord in TempFeatureWordSet else 1
            #print 'len(TempFeatureWordSet)', len(TempFeatureWordSet)
            # 同时满足在候选特征集和候选情感词集的关系对加入特征-情感词集
            FeaSenPairSet.append(
                {
                    'c_id':dpPair['s_id'],
                    's_id':dpPair['id'],
                    'feature_word':feature,
                    'sentiment_word':sentiment,
                    'relate': dpPair['w_relate']
                }
            )
            #print len(FeaSenPairSet)
            #print dpPair['s_id'], featureWord, sentimentWord
        return False
    return True


if __name__ == '__main__':
    appName = 'extract_pair_app'
    masterName = 'spark://caiwencheng-K53BE:7077'

    my_spark = SparkSession \
        .builder \
        .appName(appName) \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.jd_dp_pairs") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/jd.fea_sen_pairs_3133811") \
        .master(masterName) \
        .getOrCreate()

    # dataframe
    df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    # 测试df
    df = my_spark.createDataFrame(df.take(10000))
    print df.count
    # df->jsonrdd  纯json格式了
    originData = df.toJSON()

    # 粗糙去重
    #oriDisData = originData.distinct()
    # 收集 id,s_id,w_relate,parent,child格式json字符串（unicode）
    oriDisDataList = originData.collect()

    remainOriDisData = filter(lambda dpPair: extractSentiment(dpPair, inFeedFeatureSet, 0),oriDisDataList)
    preCount = len(remainOriDisData)
    print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}，剩余依存对数：{3}'\
        .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict), len(FeaSenPairSet), preCount)
    print preCount  #233,93(100,0)
    while True:
        remainOriDisData = filter(lambda dpPair: extractFeature(dpPair),remainOriDisData)
        print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}，剩余依存对数：{3}' \
            .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict), len(FeaSenPairSet), len(remainOriDisData))
        remainOriDisData = filter(lambda dpPair: extractSentiment(dpPair, inTempFeatureWordSet,1),remainOriDisData)
        currCount = len(remainOriDisData) #220
        print '候选特征数：{0}，候选情感词数：{1}，特征情感词数：{2}，剩余依存对数：{3}' \
            .format(len(temp_feature_word_dict), len(temp_sentiment_word_dict), len(FeaSenPairSet), currCount)
        print ('precount:%d,currcount:%d')%(preCount,currCount)
        if currCount != preCount:  # 如果剩余依存对数量不再改变，则迭代完成
            preCount = currCount
        else:
            break
    FeaSenPairSetDF = my_spark.createDataFrame(FeaSenPairSet)
    FeaSenPairSetDF.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    my_spark.stop()