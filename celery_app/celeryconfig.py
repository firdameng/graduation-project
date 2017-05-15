
# coding=utf-8

# 代理人配置 ,也可以创建rabbitmq 用户和密码
# BROKER_URL = 'amqp://guest:guest@localhost:5672'
# 详见broker setting
BROKER_URL = 'amqp://guest:guest@localhost:5672'

# 默认是utc
CELERY_TIMEZONE = 'Asia/Shanghai'

# 指定导入任务模块
CELERY_IMPORTS = {
    'celery_app.tasks'
}

# 任务结果存储在哪里 两种方式
#CELERY_RESULT_BACKEND = 'mongodb://localhost:27017/celery_one'  默认集合是celery_taskmeta

CELERY_RESULT_BACKEND = 'mongodb://localhost:27017/'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'database':'jd',
    'taskmeta_collection':'commentDps_3133811'
}


# 当然还有任务传输格式，
# CELERY_TASK_SERIALIZER = 'msgpack'

# 结果序列化传输格式
CELERY_RESULT_SERIALIZER = 'json'

# 任务可能由于网络问题被阻塞，需要设置时间限制，
# 在task中实现，注意是profork的
#CELERY_TASK_TIME_LIMIT = 3

# 结果过期时间，未处理的情况下，backend存在情况下必须使用
CELERY_TASK_RESULT_EXPIRES = 60*60*24

# 用于跟踪task状态
CELERY_TRACK_STARTED = True