# coding=utf-8
import json

from pymongo import MongoClient

from celery_app import tasks, celeryconfig


def parseComment2Dp():
    db_name = 'jd'
    coll_name = 'productId_3133811'
    client = MongoClient()
    db = client.get_database(db_name)
    collection = db.get_collection(coll_name)
    # cursor = collection.find({'_id': {'$lt': 1001}})
    cursor = collection.find().limit(10)
    for c in cursor:
        tasks.ltp_comment_np_xml.delay(c)
    client.close()


# wId 假定在 commDp范围内
def getWordFromComm(wId, commDp):
    for w in commDp:
        if w['id'] == wId:
            return w


def getDpPairSet(comment_dp_result_str):
    dpPairSet = {'SBV': [], 'VOB': [], 'ATT': [], 'CMP': []}
    comment_dp_result = json.loads(comment_dp_result_str)
    commDp = comment_dp_result.values()[0]
    commId = comment_dp_result.keys()[0]
    for w in commDp:
        # 如果主谓关系
        if w['relate'] in dpPairSet.keys():
            w_relate = w['relate']
            w_child = getWordFromComm(w['parent'], commDp)
            dpPairSet[w_relate].append(
                {
                    's_id': commId,
                    'parent': w if w_relate in ['ATT', 'SBV'] else w_child,  # 主语
                    'child': w_child if w_relate in ['ATT', 'SBV'] else w,  # 谓语
                    'w_relate': w_relate
                }
            )
            m_child = m_parent = None
            for m in commDp:
                # 如果m和w并列，则添加(w_parent,m)
                if m['relate'] == 'COO' and m['parent'] == w['id']:
                    m_parent = m
                    dpPairSet[w_relate].append(
                        {
                            's_id': commId,
                            'parent': m if w_relate in ['ATT', 'SBV'] else w_child,
                            'child': w_child if w_relate in ['ATT', 'SBV'] else w,
                            'w_relate': w_relate
                        })
                # 如果m和w_parent并列，则添加(m,w)
                if m['relate'] == 'COO' and m['parent'] == w['parent']:
                    m_child = m
                    dpPairSet[w_relate].append(
                        {
                            's_id': commId,
                            'parent': w if w_relate in ['ATT', 'SBV'] else m,
                            'child': m if w_relate in ['ATT', 'SBV'] else w,
                            'w_relate': w_relate
                        })
            if m_child and m_parent:
                dpPairSet[w_relate].append(
                    {
                        's_id': commId,
                        'parent': m_parent if w_relate in ['ATT', 'SBV'] else m_child,
                        'child': m_child if w_relate in ['ATT', 'SBV'] else m_parent,
                        'w_relate': w_relate
                    })
    return dpPairSet


def extractAllDpPairSet():
    # def isListEmpty(inList):
    #     return all(map(isListEmpty, inList)) if isinstance(inList, list) else False
    dp_db_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['database']
    dp_coll_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['taskmeta_collection']

    client = MongoClient()
    dp_pairs = client.get_database('jd').get_collection('jd_dp_pairs')
    bufferSize = 10
    allDpPairSet = {'SBV': [], 'VOB': [], 'ATT': [], 'CMP': []}
    try:
        db = client.get_database(dp_db_name)
        collection = db.get_collection(dp_coll_name)
        cursor = collection.find().limit(101)
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
                bufferSize = 10  # 重设大小
                allDpPairSet = {'SBV': [], 'VOB': [], 'ATT': [], 'CMP': []}  # 清空buffer
    finally:
        client.close()
    return allDpPairSet


if __name__ == '__main__':
    parseComment2Dp()
    #extractAllDpPairSet()
    # Set = extractAllDpPairSet()
    # for c in Set.values():
    #     for item in c:
    #         # item = item[0]
    #         print item['s_id'], item['parent']['cont'], item['parent']['pos'], item['child']['cont'], \
    #             item['child']['pos'], item['w_relate']
