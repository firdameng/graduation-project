# coding=utf-8
import json
import operator
from collections import Counter

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

    print '{0},{1},{2}'.format(tp, classified_true, actually_true)
    p = tp / float(classified_true)
    r = tp / float(actually_true)
    print '精确率 P', p
    print '召回率 R', r
    print 'F值', p * r * 2 / (p + r)



def summarizing_product_statistics(spark,product_id,database,comments_degree_collection,features_collection,
                                   product_summary_collection):

    ''' 总结产品信息，包括热点标签统计，产品好评率统计'''
    def extract_comment_degrees(c_id, c_degrees):
        c_degrees_dict = json.loads(c_degrees) if isinstance(c_degrees, unicode) else c_degrees
        degrees = []
        for s in c_degrees_dict:
            for sd in s['sDegrees']:
                s_degree = {'cId': c_id, 'sId': s['sId']}
                for k, v in sd.items():
                    s_degree[k] = v
                degrees.append(s_degree)
        return (c_id, degrees)

    def dict2row(summary):
        summary['hotCommentTagStatistics'] = json.dumps(summary['hotCommentTagStatistics'])
        summary['productCommentSummary'] = json.dumps(summary['productCommentSummary'])
        return Row(**summary)

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

    # 提取评论中情感计算后的评价多元组 cId,sId,sDegree
    degrees_rdd = c_degrees_rdd.map(lambda x: extract_comment_degrees(x['cId'], x['cDegrees'])). \
        flatMapValues(lambda y: y). \
        values()
    degrees_rdd.cache()

    # 创建degrees_df,或者list可以比较一下谁比较快
    # degrees_df = spark.createDataFrame(degrees_rdd.map(lambda x : Row(**x)))
    # degrees_df.cache()
    degrees = degrees_rdd.collect()

    # 获取热门特征及其统计数据，
    features_df = spark.read.format("com.mongodb.spark.sql.DefaultSource"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  features_collection).load()
    hot_features = features_df.sort(features_df.freq.desc()).take(100)
    hotTagStatistics = []
    for feature in hot_features:
        # 已确定的该特征相关的多元评价组数
        related_degrees = filter(lambda x: x['feature']['cont'] == feature.name, degrees)
        if not related_degrees:
            continue
        degrees_count = len(related_degrees)  # 特征评价总数
        comments_count = len(list(set(map(lambda z: z['cId'], related_degrees))))  # 相关评论总数

        good_degrees = filter(lambda y: y['degValue'] > 0, related_degrees)
        good_comments = list(set(map(lambda z: z['cId'], good_degrees)))

        bad_degrees = filter(lambda y: y['degValue'] < 0, related_degrees)
        bad_comments = list(set(map(lambda z: z['cId'], bad_degrees)))

        good_count = len(good_degrees)  # 好特征总数
        good_rate = good_count / float(degrees_count)
        # 好评率确定评价词
        if good_rate < 0.5:
            sentiment_word = sorted(Counter(map(lambda x: x['sentiment']['cont'], bad_degrees)).items(),
                                         key=operator.itemgetter(1),reverse = True)[0][0]
        else:
            sentiment_word = sorted(Counter(map(lambda x: x['sentiment']['cont'], good_degrees)).items(),
                                         key=operator.itemgetter(1),reverse = True)[0][0]
        hotTagStatistics.append(
            {
                'tFeature': feature.name,
                'tSentiment': sentiment_word,
                'tGoodRate': good_rate,
                'tCount': degrees_count,
                'tCommentCount': comments_count,
                'tGoodComment': good_comments,    #list
                'tPoorComment': bad_comments
            }
        )
    sorted(Counter(map(lambda x: x['sentiment']['cont'], hotTagStatistics)).items(),
           key=operator.itemgetter(1), reverse=True)
    for i in hotTagStatistics:
        print u'热门标签：{0}{1},好评率：{2},{3}'.format(i['tFeature'],i['tSentiment'],i['tGoodRate'],i['tCount'])
    # 产品总体评价
    identified_count = len(valid_degrees_map)
    all_good_comments = len(filter(lambda x: x[1] > 0, valid_degrees_map.items()))
    product_summary = {
        'identifiedCount': identified_count,
        'goodCount': all_good_comments,
        'goodRate': all_good_comments / float(identified_count),
    }

    summary = {
        'pId': product_id,
        'hotCommentTagStatistics': hotTagStatistics,
        'productCommentSummary': product_summary
    }
    summary_rdd = spark.sparkContext.parallelize([summary])
    product_summary_df = spark.createDataFrame(summary_rdd.map(lambda x: dict2row(x)))
    product_summary_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite"). \
        option("uri", "mongodb://127.0.0.1/").option("database", database).option("collection",
                                                                                  product_summary_collection).\
        save()

if __name__ == '__main__':
    product_id = 4297772
    database = 'jd'
    comments_degree_collection = 'comments_degree_%d' % product_id
    purged_comments_collection = 'purged_comments'
    product_summary_collection = 'product_summary_%d' % product_id
    features_collection = 'feature_word_4297772'

    app_name = '[APP] product_summary'
    master_name = 'spark://caiwencheng-K53BE:7077'

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master_name) \
        .getOrCreate()

    #assess_comment_classfication(spark,database,comments_degree_collection,purged_comments_collection)

    product_summary_collection = 'product_summary'
    summarizing_product_statistics(spark,product_id,database,comments_degree_collection,features_collection,
                                   product_summary_collection)
    pass
