# coding=utf-8


from pymongo import MongoClient

from celery_app import tasks, celeryconfig

if __name__ == '__main__':
    db_name = 'jd'
    coll_name = 'productId_3133811'
    client = MongoClient()
    db = client.get_database(db_name)
    collection = db.get_collection(coll_name)
    #cursor = collection.find({'_id': {'$lt': 1001}})
    cursor = collection.find()
    for c in cursor:
        res=tasks.ltp_comment_np_xml.delay(c)
        #res = tasks.ltp_comment_np_xml.s(c).apply_async(link_error=tasks.error_handler.s())     #异步执行，继续执行下面代码
        #print (c['_id'],res.id,res.state)


    #
    # # 回查评论是否全部被依存句法处理成功
    # des_db_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['database']
    # des_coll_name = celeryconfig.CELERY_MONGODB_BACKEND_SETTINGS['taskmeta_collection']
    #
    # # 结果集合存在
    # des_db = client.get_database(des_db_name) if des_db_name in client.database_names() else None
    # des_collection = des_db.get_collection(des_coll_name) if des_db and des_coll_name in des_db.collection_names() else None
    # if des_collection is not None:
    #
    #     # 循环查询并发送任务，知道全部成功
    #     while(True):
    #         fail_status_cursor = des_collection.find({'status':'FAILURE'})
    #         # 存在失败状态结果
    #         if fail_status_cursor.count():
    #             for fail_result in fail_status_cursor:
    #                 retry_comm_cor =collection.find({'_id':fail_result['result'][0]})
    #                 tasks.ltp_comment_np_xml.delay(retry_comm_cor.next())
    client.close()
