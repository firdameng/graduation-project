# coding=utf-8
from pyspark.sql import SparkSession

import info_collect
import pairs_extract

if __name__ == '__main__':
    '''
    毕设主程序入口
    '''
    # app_name = 'caiwencheng'
    # master_name = 'spark://caiwencheng-K53BE:7077'
    # my_spark = SparkSession \
    #     .builder \
    #     .appName(app_name) \
    #      .master(master_name) \
    #     .getOrCreate()

    product_id = 4297772
    database = 'jd'
    raw_comments_collection = 'raw_comments'

    # cback = 'fetchJSON_comment98vv49564'
    # maxsize = 20000
    # info_collect.crawl_pruduct_comments(product_id, cback, maxsize)

    # 有一定的时间间隔，等异步任务执行完
    formatted_comments_collection = 'formatted_comments'
    # # info_collect.format_comments(my_spark,product_id,database,raw_comments_collection,formatted_comments_collection)
    #
    purged_comments_collection = 'purged_comments'
    #info_collect.pre_process_comments(my_spark,database,formatted_comments_collection,purged_comments_collection)
    pairs_extract.dp_comments(database,product_id,purged_comments_collection)

