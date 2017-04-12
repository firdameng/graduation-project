# coding=utf-8
import urllib2
import cookielib
import lxml.html


# 获取验证码
def get_captcha(html):
    tree = lxml.html.fromstring(html)
    img_data = tree.cssselect('img#captcha_image')[0].get('src')


def login2():
    # 准备request的参数url,data,headers
    login_url = 'https://accounts.douban.com/login'
    post_data = 'source=None&redir=https%3A%2F%2Fwww.douban.com&form_email=1546233608%40qq.com&\
        form_password=cai4739027db*&login=%E7%99%BB%E5%BD%95'
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.04"}

    # 使用urllib2.request来构造一般request
    request = urllib2.Request(login_url , post_data , headers)

    # 构造一般opener实现对cookie，debug功能
    httpHandler = urllib2.HTTPHandler(debuglevel=1)
    httpsHandler = urllib2.HTTPSHandler(debuglevel=1)

    filename = 'cookie.txt'
    cookieJar = cookielib.MozillaCookieJar(filename)

    cookieHandler = urllib2.HTTPCookieProcessor(cookieJar)
    opener = urllib2.build_opener(httpHandler , httpsHandler , cookieHandler)

    # 用opener执行请求，得到response
    response = opener.open(request)
    cookieJar.save(ignore_discard=True , ignore_expires=True)

    # 结果
    print response.info()
    print response.code
    print response.geturl()


def login():
    filename = 'login_cookie.txt'
    url = 'https://accounts.douban.com/login'
    encode_data = 'source=None&redir=https%3A%2F%2Fwww.douban.com&form_email=1546233608%40qq.com&' \
                  'form_password=cai4739027db*&login=%E7%99%BB%E5%BD%95'

    # 声明保存cookie的对象
    cj = cookielib.MozillaCookieJar(filename)
    # 建立opener传递更多参数
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    # 打开url
    response = opener.open(url , encode_data)
    # 保存cookie
    cj.save(ignore_discard=True , ignore_expires=True)

    re_url = 'https://movie.douban.com/subject/1849031/comments?start=300&limit=20&sort=new_score&status=P'
    test_url = 'https://www.douban.com/mine/orders/'
    response = opener.open(test_url)

    print response.info()
    print response.code
    print response.geturl()

    return opener


def request(opener , url):
    # fullname = os.path.join(os.getcwd(), cookie_name)
    # print fullname
    # cj = cookielib.MozillaCookieJar()
    # cj.load(fullname)

    # print cj
    # opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    response = opener.open(url)
    assert isinstance(response , None)
    print response.info()
    print response.code
    print response.geturl()


def main():
    # opener = login()
    # request(opener, "https://movie.douban.com/subject/1849031/comments?start=300&limit=20&sort=new_score&status=P")
    login2()


if __name__ == '__main__':
    main()
