# coding=utf-8
from pymongo import MongoClient
from pymongo.errors import PyMongoError, CollectionInvalid  # alter+enter 自动导包


class DBInvalid(PyMongoError):
    ''' 抛出数据库异常'''


class Mongodb_Helper(object):
    # 初始化一个mongodb客户端，并指明哪个数据库
    def __init__(self, db=None):
        self.client = MongoClient()
        self.db = self.client.get_database(db) if db else None

    # 在该数据库db上创建一个文档集合
    def create_collection(self, name):
        if self.db is None:
            raise DBInvalid("database is None")
        return self.db.create_collection(name)

    def insert_documents(self, collection, documents):
        if collection is None:
            raise CollectionInvalid("collection is None")
        return collection.insert_many(documents)

    def close(self):
        self.client.close()


def main():
    doubanHelper = Mongodb_Helper(db='test_douban')
    try:
        print doubanHelper.db.name
        comments = doubanHelper.create_collection('movie40')
        many_comment = [{'comment-vote': 1000, 'comment-rating': 5, 'comment-text': '真心不错'}]
        doubanHelper.insert_documents(collection=comments, documents=many_comment)
        print doubanHelper.db.name
        print doubanHelper.db.get_collection(comments.name).find_one()
    finally:
        doubanHelper.close()


if __name__ == '__main__':
    # 要先启动mongod.exe，
    main()
