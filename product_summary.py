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

    # 仅关注有效情感评论中正向结果
    classified_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True)).\
        map(lambda x : (x['cId'],1 if x['cDegValue'] > 0.5 else -1)).\
        sortBy(lambda z : z[0])
    classified_degrees_rdd.cache()

    # 建立有效情感评论集的k,v字典，作为广播变量用于过滤原始评论
    classified_degrees_dict = classified_degrees_rdd.collectAsMap()
    broadcast_degrees_dict = spark.sparkContext.broadcast(classified_degrees_dict)


    # 加载原始评论，根据有效情感评论集，得到和有效情感评论相同的原始精简k,v评论集
    ori_comm_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  purged_comments_collection).load()
    # 如此得到和有效评论集对应的原始评论集，并带有相应的标签
    actually_degrees_rdd = ori_comm_df.rdd.map(lambda y: y.asDict(recursive=True)).\
        filter(lambda z : z['cId'] in broadcast_degrees_dict.value).\
        map(lambda x : (x['cId'],1 if x['score'] > 3 else -1)). \
        sortBy(lambda t: t[0])
    actually_degrees_rdd.cache()

    tp = actually_degrees_rdd.map(lambda x : x[1]).\
        zip(classified_degrees_rdd.map(lambda y : y[1])).\
        filter(lambda z: z[0] == z[1] and z[0] > 0). \
        count()
    classified_true = classified_degrees_rdd.filter(lambda x: x[1] > 0).count()
    actually_true = actually_degrees_rdd.filter(lambda x: x[1] > 0).count()
    all_count = classified_degrees_rdd.count()

    print '{0},{1},{2},{3}'.format(tp, classified_true, actually_true, all_count)
    p = tp / float(classified_true)
    r = tp / float(actually_true)
    print '精确率 P', p
    print '召回率 R', r
    print 'F值', p * r * 2 / (p + r)

    # c_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True))
    # # 假定极性值大于0.5的评论为正向，小于-0.5的为负向，过滤-0.5~0.5
    # valid_degrees_rdd = c_degrees_rdd \
    #     .filter(lambda x: x['cDegValue'] > 0.5 or x['cDegValue'] < -0.5) \
    #     .map(lambda z: (z['cId'], 1 if z['cDegValue'] > 0 else -1)) \
    #     .sortBy(lambda x: x[0])  # 注意这里cid,degree排序了
    # valid_degrees_rdd.cache()

    # 建立有效情感评论集的k,v字典，作为广播变量用于过滤原始评论
    # valid_degrees_map = valid_degrees_rdd.collectAsMap()
    # broadcast_degrees_map = spark.sparkContext.broadcast(valid_degrees_map)



    # 中评评论id
    # ori_zero_comm_map =  ori_comm_df.rdd.filter(lambda x : 1 < x.score < 4).\
    #     map(lambda y : (y.cId,1)).\
    #     collectAsMap()
    #
    # # 原始评分4,5为好评，1为差评,过滤2,3中评
    # ori_valid_comm_rdd = ori_comm_df.rdd.map(lambda y: y.asDict(recursive=True)). \
    #     filter(lambda x: x['score'] > 3 or x['score'] < 2 ). \
    #     map(lambda z: (z['cId'], 1 if z['score'] > 3 else -1)). \
    #     sortBy(lambda x: x[0])
    # ori_valid_comm_rdd.cache()
    # # 计算精确率(正向而言)
    # tp = ori_valid_comm_rdd.zip(valid_degrees_rdd). \
    #     filter(lambda z: z[0] == z[1] and z[0] > 0). \
    #     count()
    # classified_true = valid_degrees_rdd.filter(lambda x: x[1] > 0).count()
    # actually_true = ori_valid_comm_rdd.filter(lambda x: x[1] > 0).count()
    # all_count = valid_degrees_rdd.count()
    #
    # print '{0},{1},{2},{3}'.format(tp, classified_true, actually_true,all_count)
    # p = tp / float(classified_true)
    # r = tp / float(actually_true)
    # print '精确率 P', p
    # print '召回率 R', r
    # print 'F值', p * r * 2 / (p + r)


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

    # 是按特征评价多元组来的，并非是评论句子级别
    ori_tag_statistics = features_rdd.map(
        lambda x : get_hot_tag_statistics(x,database,temp_degrees_collection)).collect()

    # tag_purged_statistics = filter(
    #     lambda z : (z['tIsPos'] and z['tCount'] > 100 and z['tRate'] > 0.75) or
    #                (not z['tIsPos'] and z['tCount'] > 2 and z['tRate'] > 0.6),
    #            filter(lambda x : len(x) > 0,ori_tag_statistics))
    # tag_purged_statistics = filter(lambda x: len(x) > 0, ori_tag_statistics)

    # len(x)>0 是除去原始评论中可能不存在相关特征
    pos_tag_statistics = filter(lambda x: len(x) > 0 and x['tIsPos'], ori_tag_statistics)
    neg_tag_statistics = filter(lambda x: len(x) > 0 and not x['tIsPos'], ori_tag_statistics)

    # 正负向标签频数的平均值
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
        print u'热门标签：{0}{1},好评率：{2},总次数：{3},倾向：{4}\n正向相关评论：{5}\n负向相关评论：{6}'.\
            format(i['tFeature'],i['tSentiment'],i['tRate'],i['tCount'],i['tIsPos'],i['tGoodComment'][:10],
                   i['tBadComment'][:10])
    for i in hot_neg_tag_statistics:
        print u'热门标签：{0}{1},差评率：{2},总次数：{3},倾向：{4}\n正向相关评论：{5}\n负向相关评论：{6}'.\
            format(i['tFeature'], i['tSentiment'],i['tRate'], i['tCount'], i['tIsPos'],i['tGoodComment'][:10],
                   i['tBadComment'][:10])

    c_degrees_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  comments_degree_collection).load()
    # valid_degrees_rdd = c_degrees_df.rdd.map(lambda y: y.asDict(recursive=True)).\
    #     filter(lambda x : x['cDegValue'] != 0)
    # valid_degrees_rdd.cache()

    # 产品总体评价
    valid_comments_count = c_degrees_df.count()
    c_degrees_df.cache()
    good_comments_count = c_degrees_df.filter(c_degrees_df.cDegValue > 0.5).count()
    product_summary = {
        'identifiedCount': valid_comments_count,
        'goodCount': good_comments_count,
        'goodRate': good_comments_count / float(valid_comments_count),
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
    product_id = 3899582       #4297772，3899582
    database = 'jd'
    comments_degree_collection = 'comments_degree_%d' % product_id
    purged_comments_collection = 'purged_comments_%d' % product_id


    app_name = '[APP] product_summary'
    master_name = 'spark://caiwencheng-K53BE:7077'

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    #assess_comment_classfication(spark,database,comments_degree_collection,purged_comments_collection)

    features_collection = 'feature_word_%d' % product_id
    product_summary_collection = 'product_summary'
    temp_degrees_collections = 'temp_degrees_%d' % product_id   # 即评论中的所以特征评价对

    # extract_single_degrees(spark,database,comments_degree_collection,temp_degrees_collections)

    summarizing_product_statistics(spark,product_id,database,comments_degree_collection,temp_degrees_collections,
                                    features_collection,product_summary_collection)
    pass
