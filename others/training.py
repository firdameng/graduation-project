# coding=utf-8

import os

import jieba
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import NaiveBayes, SVMWithSGD
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint

dataDir = '/home/caiwencheng/PycharmProjects/graduation-project'
filename = 'movie_1291560.txt'
path = os.path.join(dataDir,filename)

appName = 'graduation-project'
master = 'spark://caiwencheng-K53BE:7077'


def ReduceRate(rate):
    return 1 if int(rate) > 4 else 0

def filterStopWords(line,stopwords):
    for i in line:
        if i in stopwords:
            line.remove(i)
    return line

def main():

    # 初始化环境
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    # 数据预处理（装载--》去重--》格式化）  RDD
    originData = sc.textFile(path)
    originDistinctData = originData.distinct()
    rateDocument = originDistinctData.map(lambda line: line.split('\t')). \
        filter(lambda line: len(line) == 2)

    # 统计数据基本信息 5评分为好评
    fiveRateDocument = rateDocument.filter(lambda line: int(line[0]) == 5)
    fiveRateDocument.count()


    # 假定 0,1,2,3评分为差评
    negRateDocument = rateDocument.filter(lambda line: int(line[0]) < 4)

    # 保证正负样本集一致，重新分个区,生̧成训练数̧据集 rate评分（0,1）集合，document内容（text）集合
    posRateDocument = sc.parallelize(fiveRateDocument.take(negRateDocument.count())).repartition(1)
    allRateDocument = negRateDocument.union(posRateDocument)
    allRateDocument.repartition(1)
    rate = allRateDocument.map(lambda s: ReduceRate(s[0]))
    document = allRateDocument.map(lambda s: s[1])

    # 结巴分词
    # 搜索引擎模式
    #words = document.map(lambda w: "/".join(jieba.cut_for_search(w))).map(lambda line: line.split("/"))
    # 全模式
    words = document.map(lambda w: "/".join(jieba.cut(w, cut_all=True))).map(lambda line: line.split("/"))

    # 构建停用词表，并过滤停用词
    text = words.flatMap(lambda w: w)
    wordCounts = text.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b). \
        sortBy(lambda x: x[1], ascending=False)
    stopwords = set(map(lambda x:x[0],wordCounts.take(10)))
    words = words.map(lambda w: filterStopWords(w,stopwords))


    # 训练词频矩阵 TF = term frequency
    hashingTF = HashingTF()
    tf = hashingTF.transform(words)
    tf.cache()

    # 计算IF-IDF矩阵
    idfModel = IDF().fit(tf)
    tfidf = idfModel.transform(tf)

    # 生成训练集和测试集，方法是连接两个RDD成为元组对
    zipped = rate.zip(tfidf)
    data = zipped.map(lambda line: LabeledPoint(line[0], line[1]))
    training, test = data.randomSplit([0.6, 0.4], seed=0)

    # 训练朴素贝叶斯分类模型 or SVM分类模型
    #NBmodel = NaiveBayes.train(training,1.0)
    #predictionAndLabel = test.map(lambda p: (NBmodel.predict(p.features), p.label))

    SVMmodel = SVMWithSGD.train(training, iterations=100)
    predictionAndLabel = test.map(lambda p: (SVMmodel.predict(p.features), p.label))

    accuracy = 1.0 * predictionAndLabel.filter(lambda x: 1.0 \
        if x[0] == x[1] else 0.0).count() / test.count()

    print 'test original accuracy:',accuracy
    # 分类未分类文本
    yourDocument = raw_input(u'请输入文字: '.encode("utf8"))
    yourwords = "/".join(jieba.cut_for_search(yourDocument)).split("/")

    yourtf = hashingTF.transform(yourwords)
    yourtfidf = idfModel.transform(yourtf)

    #print('NaiveBayes Model Predict:', NBmodel.predict(yourtfidf))
    print('NaiveBayes Model Predict:', SVMmodel.predict(yourtfidf))

if __name__ == '__main__':
    main()