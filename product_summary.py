# coding=utf-8
import json
import time

from pymongo import MongoClient
from pyspark import Row
from pyspark.sql import SparkSession


def assess_comment_classfication(spark,database, comments_degree_collection, purged_comments_collection):

    c_degrees_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  comments_degree_collection).load()
    c_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True))
    # 剔除评论情感值为0的数据，保留极性值大于0或小于0的
    valid_degrees_rdd = c_degrees_rdd \
        .filter(lambda x: x['cDegValue'] != 0) \
        .map(lambda z: (z['cId'], 1 if z['cDegValue'] > 0 else -1)) \
        .sortBy(lambda x: x[0])  # 注意这里cid,degree排序了
    valid_degrees_rdd.cache()

    # 建立有效情感评论集的k,v字典，作为广播变量用于过滤原始评论
    valid_degrees_map = valid_degrees_rdd.collectAsMap()
    broadcast_degrees_map = spark.sparkContext.broadcast(valid_degrees_map)

    # 加载原始评论，根据有效情感评论集，得到和有效情感评论相同的原始精简k,v评论集
    ori_comm_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  purged_comments_collection).load()
    # 原始评分4,5为好评，其余为差评
    ori_valid_comm_rdd = ori_comm_df.rdd.map(lambda y: y.asDict(recursive=True)). \
        filter(lambda x: x['cId'] in broadcast_degrees_map.value). \
        map(lambda z: (z['cId'], 1 if z['score'] > 3 else -1)). \
        sortBy(lambda x: x[0])
    ori_valid_comm_rdd.cache()
    # 计算精确率(正向而言)
    tp = ori_valid_comm_rdd.map(lambda x: x[1]). \
        zip(valid_degrees_rdd.map(lambda y: y[1])). \
        filter(lambda z: z[0] == z[1] and z[0] > 0). \
        count()
    classified_true = valid_degrees_rdd.filter(lambda x: x[1] > 0).count()
    actually_true = ori_valid_comm_rdd.filter(lambda x: x[1] > 0).count()
    all_count = valid_degrees_rdd.count()

    print '{0},{1},{2},{3}'.format(tp, classified_true, actually_true,all_count)
    p = tp / float(classified_true)
    r = tp / float(actually_true)
    print '精确率 P', p
    print '召回率 R', r
    print 'F值', p * r * 2 / (p + r)


def extract_single_degrees(spark,database,comments_degree_collection,temp_degrees_collections):

    def extract_comment_degrees(c_id, c_degrees):
        c_degrees_dict = json.loads(c_degrees) if isinstance(c_degrees, unicode) else c_degrees
        degrees = []
        for s in c_degrees_dict:
            for sd in s['sDegrees']:
                s_degree = {'cId': c_id}
                for k, v in sd.items():
                    s_degree[k] = {} if v == None else v  #考虑到不存在否定副词
                degrees.append(s_degree)
        return (c_id, degrees)

    c_degrees_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  comments_degree_collection).load()
    c_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True))
    # -------------同时应当过滤评价极性值为0的评价多元组----------------
    degrees_rdd = c_degrees_rdd.map(lambda x: extract_comment_degrees(x['cId'], x['cDegrees'])). \
        flatMapValues(lambda y: y). \
        values(). \
        filter(lambda z: z['degValue'] != 0)

    degrees_df = spark.createDataFrame(degrees_rdd.map(lambda x : Row(**x)))

    degrees_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).\
        option("collection",temp_degrees_collections).\
        save()


def summarizing_product_statistics(spark,product_id,database,comments_degree_collection,temp_degrees_collection,
                                   features_collection,product_summary_collection):
    def dict2row(summary):
        for k,v in summary.items():
            summary[k] = v if k == 'pId' else json.dumps(summary[k])
        return Row(**summary)

    def get_hot_tag_statistics(feature,database, temp_degrees_collection):

        t1 = time.time()
        client = MongoClient()
        try:
            db = client.get_database(database)
            tmp_deg_collection = db.get_collection(temp_degrees_collection)
            cursor = tmp_deg_collection.find({'feature.cont':feature.name})

            t2 = time.time()
            print 't2-t1',t2-t1

            related_degrees = []
            good_degrees = []
            good_sentiment_words = {}
            bad_degrees = []
            bad_sentiment_words = {}

            for deg in cursor:

                senti_word = deg['sentiment']['cont'] if not deg['negAdv'] else \
                    deg['negAdv']['cont'] + deg['sentiment']['cont']
                related_degrees.append(deg['cId'])

                if deg['degValue'] > 0 :
                    good_degrees.append(deg['cId'])
                    good_sentiment_words[senti_word] = good_sentiment_words[senti_word] + 1 \
                        if senti_word in good_sentiment_words else 1
                else:
                    bad_degrees.append(deg['cId'])
                    bad_sentiment_words[senti_word] = bad_sentiment_words[senti_word] + 1 \
                        if senti_word in bad_sentiment_words else 1
        finally:
            client.close()

        if not related_degrees:             #没找到相关特征
            return {}

        degrees_count = len(related_degrees)  # 有效特征评价多元组总数
        # 计算好评率
        good_count = len(good_degrees)  # 好特征总数
        good_rate = good_count / float(degrees_count)

        #根据好评率确定评价情感词
        if good_rate < 0.5:
            sentiment_word = sorted(bad_sentiment_words.items(), key=lambda x : x[1], reverse=True)[0][0]
            rate = 1 - good_rate
            isPos = False
        else:
            sentiment_word = sorted(good_sentiment_words.items(), key=lambda x : x[1], reverse=True)[0][0]
            rate = good_rate
            isPos = True

        t4 = time.time()
        print 't4-t2', t4 - t2
        return {
                    'tFeature': feature.name,
                    'tSentiment': sentiment_word,
                    'tRate': rate,
                    'tIsPos': isPos,
                    'tCount': degrees_count,
                    'tCommentCount': len(set(related_degrees)),
                    'tGoodComment': list(set(good_degrees)),  # list
                    'tBadComment': list(set(bad_degrees))
                }

    features_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  features_collection).load()
    features_rdd = features_df.sort(features_df.freq.desc()).rdd
    features_rdd.cache()
    # temp = []
    # for t in features_rdd.take(5):
    #     temp.append(get_hot_tag_statistics(t,database,temp_degrees_collection))

    # 起码的基准是好/差评率0.75，评论数满足正向特征的评论起码大于100，负向特征评论起码大于10
    ori_tag_statistics = features_rdd.map(
        lambda x : get_hot_tag_statistics(x,database,temp_degrees_collection)).collect()

    # tag_purged_statistics = filter(
    #     lambda z : (z['tIsPos'] and z['tCount'] > 100 and z['tRate'] > 0.75) or
    #                (not z['tIsPos'] and z['tCount'] > 2 and z['tRate'] > 0.6),
    #            filter(lambda x : len(x) > 0,ori_tag_statistics))
    # tag_purged_statistics = filter(lambda x: len(x) > 0, ori_tag_statistics)

    pos_tag_statistics = filter(lambda x: len(x) > 0 and x['tIsPos'], ori_tag_statistics)
    neg_tag_statistics = filter(lambda x: len(x) > 0 and not x['tIsPos'], ori_tag_statistics)

    pos_tag_mean_count = reduce(lambda x,y : x + y,map(lambda x : x['tCount'],pos_tag_statistics)) / \
                         len(pos_tag_statistics)
    neg_tag_mean_count = reduce(lambda x, y: x + y, map(lambda x: x['tCount'], neg_tag_statistics)) / \
                         len(neg_tag_statistics)

    hot_pos_tag_statistics =sorted(
        filter(lambda x : x['tCount'] > pos_tag_mean_count,pos_tag_statistics),
        key = lambda x : x['tRate'],reverse = True)[:100]
    hot_neg_tag_statistics = sorted(
        filter(lambda x: x['tCount'] > neg_tag_mean_count, neg_tag_statistics),
        key=lambda x: x['tRate'], reverse=True)[:100]

    # 打印测试一下
    for i in hot_pos_tag_statistics:
        print u'热门标签：{0}{1},好评率：{2},总次数：{3},倾向：{4}\n正向评论有：{5}\n负向评论有：{6}'.\
            format(i['tFeature'],i['tSentiment'],i['tRate'],i['tCount'],i['tIsPos'],i['tGoodComment'][:10],
                   i['tBadComment'][:10])
    for i in hot_neg_tag_statistics:
        print u'热门标签：{0}{1},差评率：{2},总次数：{3},倾向：{4}\n正向评论有：{5}\n负向评论有：{6}'.\
            format(i['tFeature'], i['tSentiment'],i['tRate'], i['tCount'], i['tIsPos'],i['tGoodComment'][:10],
                   i['tBadComment'][:10])

    c_degrees_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  comments_degree_collection).load()
    valid_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True)).filter(lambda x : x['cDegValue'] != 0)
    valid_degrees_rdd.cache()

    # 产品总体评价
    identified_count = valid_degrees_rdd.count()
    all_good_comments = valid_degrees_rdd.filter(lambda x: x['cDegValue'] > 0).count()
    product_summary = {
        'identifiedCount': identified_count,
        'goodCount': all_good_comments,
        'goodRate': all_good_comments / float(identified_count),
    }
    print product_summary['goodRate']


    summary = {
        'pId': product_id,
        'hotPosTagStatistics': hot_pos_tag_statistics,
        'hotNegTagStatistics':hot_neg_tag_statistics,
        'productCommentSummary': product_summary
    }
    summary_rdd = spark.sparkContext.parallelize([summary])
    product_summary_df = spark.createDataFrame(summary_rdd.map(lambda x: dict2row(x)))
    product_summary_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  product_summary_collection).\
        save()

if __name__ == '__main__':
    product_id = 4297772       #4297772，3899582
    database = 'jd'
    comments_degree_collection = 'comments_degree_%d' % product_id
    purged_comments_collection = 'purged_comments_%d' % product_id
    product_summary_collection = 'product_summary_%d' % product_id
    features_collection = 'feature_word_%d' % product_id

    app_name = '[APP] product_summary'
    master_name = 'spark://caiwencheng-K53BE:7077'

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    #assess_comment_classfication(spark,database,comments_degree_collection,purged_comments_collection)

    product_summary_collection = 'product_summary'
    temp_degrees_collections = 'temp_degrees_%d' % product_id   # 即评论中的所以特征评价对

    summarizing_product_statistics(spark,product_id,database,comments_degree_collection,temp_degrees_collections,
                                    features_collection,product_summary_collection)

    #extract_single_degrees(spark,database,comments_degree_collection,temp_degrees_collections)
    pass
