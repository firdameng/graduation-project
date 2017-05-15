# coding=utf-8
# 抽取评价多元组
'''
 commFeaSenPairSet={c_id,fea_sen_pairs} 其中fea_sen_Pairs=[{s_id,feature,sentiment} ...]
 
 # 评论句是逐句依存句法分析的，是有序的
 commDpResult={key,[[]...]}  or {c_id,c_dp_result},c_dp_result = [ s_dp... ],s_dp=[word1...]
 
 # 情况不一样不能同上用list追加
 evalMulTupleSet = {c_id,c_eval_mul_tuples},其中c_eval_mul_tuples={ s_id:s_eval_mul_tuples,...}
                                               s_eval_mul_tuples=[{ feature,deg_adj,sentiment} ...]
'''
from pymongo import MongoClient


def isAdvConj(w):
    return False
def isNegAdv(w):
    return False

def del_EMTuple_Lt_Sid(eval_mul_tuples,sid):
    pass




def extract_evalMulTupleSet(commFeaSenPairSet,commDpResult):
    # 同一评论中的特征情感对，按分句顺序排列  sorted_x = sorted(x.items(), key=operator.itemgetter(0))
    sortedCommFeaSenPairSet = sorted(commFeaSenPairSet['fea_sen_pairs'],key=lambda it:it['s_id'])
    # 待返回的评价多元组集
    c_eval_mul_tuples = {}
    for fea_sen_Pair in sortedCommFeaSenPairSet:
        # 获取特征情感对所在分句依存关系对
        s_id = fea_sen_Pair['s_id']
        s_dp_result = commDpResult['c_dp_result'][s_id]
        isHead = True
        # 待寻找的否定词
        deg_adj = None
        # 遍历当前特征情感对所在依存句法结果集
        for w in s_dp_result:

            # 判断是否首单词
            if isHead :
                isHead = False
                # 进一步判断是否是是转折词
                if isAdvConj(w['cont']):
                    del_EMTuple_Lt_Sid(s_id,c_eval_mul_tuples)
            else:
                # 如果当前词为情感词的否定副词
                if w['relate'] == 'ADV' and w['parent'] == fea_sen_Pair['sentiment']['id'] and \
                    isNegAdv(w['cont']):
                    # 判断当前不否定词为None，则将w付给deg_adj，否则双重否定置为None
                    deg_adj = w if not deg_adj else None
        # O(1)访问字典，遍历完当前依存对所在评论分句，构造并加入该分句的评价多元组集中
        if s_id not in c_eval_mul_tuples:
            c_eval_mul_tuples[s_id] = []
        else:
            c_eval_mul_tuples[s_id].append(
                {
                    'feature':fea_sen_Pair['feature'],
                    'deg_adj':deg_adj,
                    'sentiment':fea_sen_Pair['sentiment']
                }
            )
    return  {
        'c_id':commFeaSenPairSet['c_id'],
        'c_eval_mul_tuples':c_eval_mul_tuples
    }


if __name__ == '__main__':
    # 不能用spark全部加载内存，取多条再写回

    client = MongoClient()
    eval_mul_tuples= client.get_database('jd').get_collection('eval_mul_tuples_3133811')
    fea_sen_pairs = client.get_database('jd').get_collection('fea_sen_pairs_3133811')
    bufferSize = 100

    try:
        # 查询特征情感对集合
        cursor = fea_sen_pairs.find()
        while True:
            bufferSize -= 1
            print bufferSize
            try:
                dpPairSet = getDpPairSet(cursor.next()['result'])
            except:
                # 遍历完了也要先写入数据库再退出
                map(lambda l: dp_pairs if not l else dp_pairs.insert_many(l), allDpPairSet.values())
                print '遍历结束'
                break
            for k, v in dpPairSet.items():
                allDpPairSet[k] += v
            if bufferSize == 0:
                print '开始插入'
                map(lambda l: dp_pairs if not l else dp_pairs.insert_many(l), allDpPairSet.values())
                bufferSize = 100  # 重设大小
                allDpPairSet = {'SBV': [], 'VOB': [], 'ATT': [], 'CMP': []}  # 清空buffer
    finally:
        client.close()
