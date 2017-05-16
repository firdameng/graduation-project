# coding=utf-8

#该模块用于产品评论信息采集


def crawl_pruduct_comments(id,cback):
    '''
    
    :param id:   #  如 4297772 诺基亚6   3133811 iphone7lus
    :param cback: @ 如fetchJSON_comment98vv49564,fetchJSON_comment98vv75201(对应上)
    :return: 
    '''
    base_url = 'http://club.jd.com/comment/skuProductPageComments.action?' \
               'callback=@&productId=#&score=0&sortType=5&' \
               'page=$&pageSize=10&isShadowSku=0'.replace('@',cback).replace('#',str(id))


