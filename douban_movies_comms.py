# coding=utf-8

import threading
from urllib import urlencode
from urlparse import parse_qs, urlsplit, urlunsplit
from time import ctime, time  # 测试时间
import lxml.html

import mongo_cache
from downloader import Downloader, DEFAULT_RETRIES
from mongo_helper import Mongodb_Helper

COOKIE = {'dbcl2': '110626009:ZBpKPngkvYA'}
DATABASE_NAME = 'douban_movie_comments'

def set_query_parameter(url, param_name, param_value):
    scheme, netloc, path, query_string, fragment = urlsplit(url)
    query_params = parse_qs(query_string)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return urlunsplit((scheme, netloc, path, new_query_string, fragment))


def task(base_url, start, stop, db_helper, col):

    cookie = {'Cookie': '%s=%s' % (k, v) for k, v in COOKIE.items()}
    downloader = Downloader(cache=mongo_cache.MongoCache(),cookie=cookie)
    for i in range(start, stop, 20):
        new_url = set_query_parameter(base_url, 'start', i)

        # ---------------IO流中的输入流----------------------------------------
        html ,code = downloader(new_url)
        if code != 200:
            print u'访问出错...'
            break
        # -------------格式化处理--------------------------------------------
        comm_elements = lxml.html.fromstring(html).cssselect('div#comments .comment')
        comments = []
        for e in comm_elements:
            # 构建mongodb中comment文档
            comment = {}
            comment['comment-vote'] = int(e.cssselect('h3 span.comment-vote span.votes')[0].text_content())
            if not e.cssselect('h3 span.comment-info span.rating') :
                comment['comment-roting'] = 0
            else:
                comment['comment-roting'] = int(e.cssselect('h3 span.comment-info span.rating')[0].get('class').
                                            split(' ')[0][-2])
            comment['comment-text'] = e.cssselect('p')[0].text_content().encode('utf-8')
            comments.append(comment)
        # ----------IO流中的输出流--------------------------------------------
        print len(comments)
        try:
            db_helper.insert_documents(collection=col, documents=comments)
        finally:
            db_helper.close()
        print 'finish %d th download' % (i / 20)


def main():
    base_url = 'https://movie.douban.com/subject/1291560/comments?start=0&limit=20&sort=new_score&status=P'
    movie_id = base_url.split('/')[4]
    comm_sum = 10000
    thread_sum = 2
    comments_one_thread = comm_sum / thread_sum
    threads = []
    db_douban_helper = Mongodb_Helper(db=DATABASE_NAME)
    comm_collection = db_douban_helper.create_collection('movie_' + movie_id)

    for t in range(thread_sum):
        start = t * comments_one_thread
        stop = (t + 1) * comments_one_thread
        thread = threading.Thread(target=task, args=(base_url, start, stop, db_douban_helper, comm_collection))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    print 'start:%s' % ctime()
    s = time()
    main()
    print 'end:%s' % ctime()
    print  'cost:', time() - s
