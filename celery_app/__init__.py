# coding=utf-8
from celery import Celery

# 创建celery实例和加载配置模块

app = Celery('graduation_proj')
app.config_from_object('celery_app.celeryconfig')