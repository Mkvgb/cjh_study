# -*- encoding: utf-8 -*-
"""
@File       :   flaskbase.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/29 16:31
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import datetime

from flask import Flask
from flask_apscheduler import APScheduler
from flask import views, request

def testfunc():
    pass


class TestGetApi(views.MethodView):
    def get(self):
        get_data = request.args.to_dict()
        testfunc()
        return_json = {}
        return return_json



class TestPostApi(views.MethodView):
    def post(self):
        get_data = request.args.to_dict()
        get_file = request.files.get('file_path')
        testfunc()
        return_json = {}
        return return_json


class FlaskAppInit(object):
    def __init__(self):
        self.app = Flask(__name__)
        self.init_router()          # 接口任务，全起
        self.init_apscheduler()     # 定时任务，全起

    @property
    def flask_app(self):
        return self.app

    def init_conf(self):        # 没调用怎么生效的？
        self.app.config['JSON_AS_ASCII'] = False       # jsonify返回的json串打印中文
        self.app.config['SCHEDULER_EXECUTORS'] = {'default': {'type': 'threadpool', 'max_workers': 5}}

    def init_router(self):
        """API路由配置"""
        self.app.add_url_rule('/test_get_api', view_func=TestGetApi.as_view('test_get_api'))
        self.app.add_url_rule('/test_post_api', view_func=TestPostApi.as_view('test_post_api'))

    def init_apscheduler(self):
        """定时任务"""
        pass
        scheduler = APScheduler()
        scheduler.init_app(self.app)
        scheduler.add_job(func=testfunc, trigger='cron', hour=0, minute=0, id='apfunc')   # 定时任务
        scheduler.start()


# print(FlaskAppInit().app.config)
conf = {'ENV': 'production',
        'DEBUG': False,
        'TESTING': False,
        'PROPAGATE_EXCEPTIONS': None,
        'PRESERVE_CONTEXT_ON_EXCEPTION': None,
        'SECRET_KEY': None,
        'PERMANENT_SESSION_LIFETIME': datetime.timedelta(days=31),
        'USE_X_SENDFILE': False,
        'SERVER_NAME': None,
        'APPLICATION_ROOT': '/',
        'SESSION_COOKIE_NAME': 'session',
        'SESSION_COOKIE_DOMAIN': None,
        'SESSION_COOKIE_PATH': None,
        'SESSION_COOKIE_HTTPONLY': True,
        'SESSION_COOKIE_SECURE': False,
        'SESSION_COOKIE_SAMESITE': None,
        'SESSION_REFRESH_EACH_REQUEST': True,
        'MAX_CONTENT_LENGTH': None,
        'SEND_FILE_MAX_AGE_DEFAULT': datetime.timedelta(seconds=43200),
        'TRAP_BAD_REQUEST_ERRORS': None,
        'TRAP_HTTP_EXCEPTIONS': False,
        'EXPLAIN_TEMPLATE_LOADING': False,
        'PREFERRED_URL_SCHEME': 'http',
        'JSON_AS_ASCII': True,
        'JSON_SORT_KEYS': True,
        'JSONIFY_PRETTYPRINT_REGULAR': False,
        'JSONIFY_MIMETYPE': 'application/json',
        'TEMPLATES_AUTO_RELOAD': None,
        'MAX_COOKIE_SIZE': 4093}