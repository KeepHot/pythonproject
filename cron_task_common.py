import time
import datetime

import requests
import json

from loguru import logger
import os

from Crypto.Cipher import AES
import base64
from Crypto.Random import random


class CronTaskInDolphinScheduler:
    headers_json = {'Content-Type': 'application/json',
                    'client-id': 'flq43ncD'}
    trace_id = str((round(time.time() * 1000)))

    url_open_falcon = "http://127.0.0.1:1988/v1/push"

    log_path = 'C:/Users/kevinliu/Desktop/data/logs/cloud-cronServiceInDolphin/'
    count_path = 'C:/Users/kevinliu/Desktop/data/logs/cloud-cronServiceInDolphin/metric/'

    ts = int(time.time())
    value_flag = "success"

    # 初始化类对象
    def __init__(self, url_appserver_cron_task, method, step, **kwargs):
        self.url_appserver_cron_task = url_appserver_cron_task
        self.method = method
        self.step = step

        # 定时任务的data默认为空值，如有需求可传入
        kwargs.setdefault('data', {})
        self.data = kwargs['data']
        # 定时任务需要服务方提供异步线程池，timeout 默认为5，如果确有需求可更改
        kwargs.setdefault('timeout', 5)
        self.timeout = kwargs['timeout']

        # 初始化日志对象
        logger.add(
            self.log_path + self.method + '/' + 'cloud-cronServiceInDolphin.' + datetime.datetime.utcnow().strftime(
                "%Y-%m-%d") + '.log')

    # 请求appServer，并推送数据至open-falcon
    def schedule_common_cron_task(self):
        try:
            data_json = self.common_params_to_json()
            logger.info(
                'Analysis.send, ' + 'traceId:{traceId}, method:{method}, data:{data}',
                traceId=self.trace_id, method=self.method, data=data_json)
            response = requests.post(url=self.url_appserver_cron_task, headers=self.headers_json,
                                     data=data_json, timeout=self.timeout)
            response_code = dict.get(response.json(), 'code')
            if response_code != 0:
                self.value_flag = "failed"
            logger.info(
                'Analysis.recv, ' + 'traceId:{traceId}, method:{method}, response:{response}',
                traceId=self.trace_id, method=self.method, data=data_json, response=response.json())
        except Exception as e:
            self.value_flag = "failed"
            logger.exception(
                'traceId:{traceId}, method:{method}, exception:', e, traceId=self.trace_id, method=self.method)
        finally:
            self.push_data_to_open_falcon(value_flag=self.value_flag)

    # 将data转为json格式post请求appServer,common包中request请求context并集
    def common_params_to_json(self):
        scheduled_json = json.dumps(
            {
                "data": self.data,
                "context": {
                    "method": self.method,
                    "traceId": self.trace_id,
                    "token": self.encrypt_method_as_token(),
                    "accountID": 123456,
                    "timeZone": "cronTaskInDolphin",
                    "acceptLanguage": "cronTaskInDolphin",
                    "phoneOS": "cronTaskInDolphin",
                    "phoneBrand": "cronTaskInDolphin",
                    "appVersion": "cronTaskInDolphin",
                    "osInfo": "cronTaskInDolphin",
                    "clientInfo": "cronTaskInDolphin",
                    "clientType": "cronTaskInDolphin",
                    "clientVersion": "cronTaskInDolphin",
                    "terminalId": "cronTaskInDolphin",
                    "accessToken": self.encrypt_method_as_token(),
                    "sign": "cronTaskInDolphin",
                    "appId": "cronTaskInDolphin",
                    "systemTimestamp": 16232687
                }
            }
        )
        return scheduled_json

    # 定时请求的失败与总次数统计并写出
    def count_failed_or_total(self, value_flag, total_count, failed_count):
        json_path = self.count_path + self.method + "_count.json"

        json_count = self.init_and_get_count(json_path=json_path, total_count=total_count, failed_count=failed_count)
        old_failed_count = dict.get(json_count, failed_count)
        new_failed_count = old_failed_count + 1
        new_total_count = dict.get(json_count, total_count) + 1

        with open(json_path, mode='w', encoding='utf-8') as dump_f:
            if value_flag == "success":
                str_count = json.dumps({total_count: new_total_count,
                                        failed_count: old_failed_count}, indent=4)
            else:
                str_count = json.dumps({total_count: new_total_count,
                                        failed_count: new_failed_count}, indent=4)
            json.dump(str_count, dump_f)
            logger.info('method:{method}, metric-data:{data}', method=self.method, data=json.loads(str_count))
            return str_count

    # 无则初始化metric路径下count文件，有则读取文件中的值
    def init_and_get_count(self, json_path, total_count, failed_count):

        if not os.path.exists(self.count_path):
            os.mkdir(self.count_path)

        if os.path.exists(json_path):
            with open(json_path, mode='r', encoding='utf-8') as load_f:
                str_data = json.load(load_f)
                json_data = json.loads(str_data)
                logger.info(
                    '{method}_count.json:{data}', method=self.method, data=json_data)
                return json_data
        else:
            with open(json_path, mode='w', encoding='utf-8') as dump_f:
                str_count = json.dumps({total_count: 0,
                                        failed_count: 0}, indent=4)
                json.dump(str_count, dump_f)
                json_count = json.loads(str_count)
                logger.info(
                    'create {method}_count.json success, init metric data:{data}',
                    method=self.method, data=json_count)
                return json_count

    # 推送自定义数据给服务所在本机的open-falcon接口
    def push_data_to_open_falcon(self, value_flag):
        total_count = self.method + "TotalCount"
        failed_count = self.method + "FailedCount"

        metric_count = json.loads(self.count_failed_or_total(value_flag,
                                                             total_count=total_count, failed_count=failed_count))
        push_total_count = dict.get(metric_count, total_count)
        push_failed_count = dict.get(metric_count, failed_count)

        if value_flag == "success":
            push_metric_value = 0  # 如果请求成功，推送value为0
        else:
            push_metric_value = 1  # 如果请求成功，推送value为1

        payload = [
            {
                'endpoint': 'CronTaskInDolphinScheduler',  # 监控节点，比如主机名，服务名
                'metric': self.method + '.failedCount',  # 监控项，数据采集项，指标，比如 cpu.load
                'timestamp': self.ts,  # 时间戳
                'step': self.step,  # 采集的步距，比如60秒采集一次
                'value': push_metric_value,  # 监控指标的值,用于设置阈值报警
                'counterType': 'GAUGE',  # 默认
                'tags': ''  # tag是为了区分 metric，也用于做筛选，因为有些metric一样，但是tag不一样，就2个监控项
            },
            {
                'endpoint': 'CronTaskInDolphinScheduler',
                'metric': self.method + '.total',
                'timestamp': self.ts,
                'step': self.step,
                'value': push_total_count,  # 定时任务总请求次数
                'counterType': 'GAUGE',
                'tags': ''  # tag必须是键值对
            },
            {
                'endpoint': 'CronTaskInDolphinScheduler',
                'metric': self.method + '.failedTotal',
                'timestamp': self.ts,
                'step': self.step,
                'value': push_failed_count,  # 定时任务总失败请求次数
                'counterType': 'GAUGE',
                'tags': ''  # tag必须是键值对
            }
        ]

        # try:
        #     # 把数据提交open-falcon agent的接口，如果本机没有，也可以提交给一个本机可访问的地址，定义好endpoint就可
        #     open_falcon_data = json.dumps(payload)
        #     logger.info('push data: {data} ', data=open_falcon_data)
        #     push_data = requests.post(url=self.url_open_falcon, data=open_falcon_data, json=self.headers_json,
        #                               timeout=2)
        #     if push_data.status_code == 200:
        #         logger.info("upload data to falcon successfully")
        #     elif push_data.status_code == 404:
        #         logger.info("open-falcon url doesn't exit")
        #     elif push_data.status_code == 500:
        #         logger.info("open-falcon server error, please check open-falcon status")
        #     else:
        #         logger.info("upload data failed, please check")
        # except Exception as e:
        #     logger.exception("call open falcon push api failed, exception:", e)

    # AES加密方法名用于鉴权
    def encrypt_method_as_token(self):
        key = '@#$yymmxxkkyuilm'.encode('utf-8')
        raw = self.method.encode('utf-8')
        iv = self.get_random_iv()
        cipher = AES.new(key, AES.MODE_CFB, iv, segment_size=128)
        encrypted = cipher.encrypt(raw)
        return base64.urlsafe_b64encode(iv + encrypted).decode('ascii')

    # 获取AES/CFB/NoPadding的16位随机iv偏移量
    @staticmethod
    def get_random_iv():

        base_str = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
        iv_str = ''
        for _ in range(16):
            iv_str += random.choice(base_str)

        iv = iv_str.encode('utf-8')
        return iv

# import time
# import datetime
#
# import requests
# import json
#
# from loguru import logger
# import os
#
# from Crypto.Cipher import AES
# import base64
# from Crypto.Random import random
#
#
# class CronTaskInDolphinScheduler:
#     headers_json = {'Content-Type': 'application/json'}
#     traceId = str((round(time.time() * 1000)))
#
#     CIRCULAR_PUSH_METHOD = "executeCircularPush"
#     EMAIL_AUTO_SEND_METHOD = "scheduleEmailSend"
#     FAILED_COUNT = "FailedCount"
#     TOTAL_COUNT = "TotalCount"
#
#     log_path = 'C:/Users/rogercai/Desktop/data/logs/cloud-cronService/'
#     count_path = 'C:/Users/rogercai/Desktop/data/cloud-cronService-count/'
#
#     ts = int(time.time())
#     push_success_value = 0
#     push_failed_value = 1
#
#     def __init__(self, url_appserver_cron_task, method, url_open_falcon, step, **kwargs):
#         self.url_appserver_cron_task = url_appserver_cron_task
#         self.method = method
#
#         self.url_open_falcon = url_open_falcon
#         self.step = step
#
#         kwargs.setdefault('minutes', None)
#         kwargs.setdefault('time', None)
#         self.minutes = kwargs['minutes']
#         self.time = kwargs['time']
#
#     # 初始化日志对象
#     logger.add(log_path + 'cloud-cronService.' + datetime.datetime.utcnow().strftime("%Y-%m-%d-%H.0") + '.log')
#
#     def schedule_common_cron_task(self):
#         try:
#             data_json = self.common_params_to_json()
#             logger.info(
#                 'the current timestamp: {traceId}, run this current method: {method},\nwill send this data: {data}',
#                 traceId=self.traceId, method=self.method, data=data_json)
#             requests.post(url=self.url_appserver_cron_task, headers=self.headers_json,
#                           data=data_json, timeout=5)
#             self.push_data_to_open_falcon("success")
#         except Exception as e:
#             self.count_failed_or_total(count_flag=self.FAILED_COUNT)
#             logger.error(
#                 'the current timestamp: {traceId}, run this current method: {method},\nget this exception: {exception}',
#                 traceId=self.traceId, method=self.method, exception=e)
#             self.push_data_to_open_falcon("failed")
#         finally:
#             self.count_failed_or_total(count_flag=self.TOTAL_COUNT)
#
#     def common_params_to_json(self):
#         if self.method == self.CIRCULAR_PUSH_METHOD:
#             scheduled_json = json.dumps(
#                 {
#                     "data": {
#                         "minutes": self.minutes
#                     },
#                     "context": {
#                         "method": self.method,
#                         "traceId": self.traceId,
#                         "token": self.encrypt_method_as_token(),
#                         "accountID": "cronTaskInDs",
#                         "timeZone": "cronTaskInDs",
#                         "acceptLanguage": "cronTaskInDs",
#                         "phoneOS": "cronTaskInDs",
#                         "phoneBrand": "cronTaskInDs",
#                         "appVersion": "cronTaskInDs"
#                     }
#                 }
#             )
#         elif self.method == self.EMAIL_AUTO_SEND_METHOD:
#             scheduled_json = json.dumps(
#                 {
#                     "data": {
#                         "time": self.time
#                     },
#                     "context": {
#                         "method": self.method,
#                         "traceId": self.traceId,
#                         "token": self.encrypt_method_as_token(),
#                         "accountID": "cronTaskInDs",
#                         "timeZone": "cronTaskInDs",
#                         "acceptLanguage": "cronTaskInDs",
#                         "phoneOS": "cronTaskInDs",
#                         "phoneBrand": "cronTaskInDs",
#                         "appVersion": "cronTaskInDs"
#                     }
#                 }
#             )
#         else:
#             scheduled_json = json.dumps(
#                 {
#                     "data": {
#                     },
#                     "context": {
#                         "method": self.method,
#                         "traceId": self.traceId,
#                         "token": self.encrypt_method_as_token(),
#                         "accountID": "cronTaskInDs",
#                         "timeZone": "cronTaskInDs",
#                         "acceptLanguage": "cronTaskInDs",
#                         "phoneOS": "cronTaskInDs",
#                         "phoneBrand": "cronTaskInDs",
#                         "appVersion": "cronTaskInDs"
#                     }
#                 }
#             )
#         return scheduled_json
#
#     def count_failed_or_total(self, count_flag):
#         json_path = self.count_path + self.method + "_count.json"
#         total_count = self.method + self.TOTAL_COUNT
#         failed_count = self.method + self.FAILED_COUNT
#
#         json_count = self.init_and_get_count(json_path=json_path, total_count=total_count, failed_count=failed_count)
#         old_total_count = dict.get(json_count, total_count)
#         new_failed_count = dict.get(json_count, failed_count) + 1
#
#         new_total_count = dict.get(json_count, total_count) + 1
#         old_failed_count = dict.get(json_count, failed_count)
#
#         with open(json_path, mode='w', encoding='utf-8') as dump_f:
#             if count_flag == self.FAILED_COUNT:
#                 str_count = json.dumps({total_count: old_total_count,
#                                         failed_count: new_failed_count}, indent=4)
#             else:
#                 str_count = json.dumps({total_count: new_total_count,
#                                         failed_count: old_failed_count}, indent=4)
#             json.dump(str_count, dump_f)
#             logger.info(
#                 'update this count: {count} of current method: {method} success,\nget new count data: {data}',
#                 count=count_flag, method=self.method, data=str_count)
#
#     def init_and_get_count(self, json_path, total_count, failed_count):
#
#         if os.path.exists(json_path):
#             with open(json_path, mode='r', encoding='utf-8') as load_f:
#                 str_data = json.load(load_f)
#                 json_data = json.loads(str_data)
#                 logger.info(
#                     'get data in {method}_count.json: \n{data}', method=self.method, data=str_data)
#                 return json_data
#         else:
#             with open(json_path, mode='w', encoding='utf-8') as dump_f:
#                 str_count = json.dumps({total_count: 0,
#                                         failed_count: 0}, indent=4)
#                 json.dump(str_count, dump_f)
#                 json_count = json.loads(str_count)
#                 logger.info(
#                     'create {method}_count.json file and init success,\nget init count data: {data}',
#                     method=self.method, data=str_count)
#                 return json_count
#
#     def push_data_to_open_falcon(self, whether_success):
#         if whether_success == "success":
#             push_value = self.push_success_value  # 如果请求成功，推送value为0
#         else:
#             push_value = self.push_failed_value  # 如果请求失败，推送value为1
#
#         payload = [
#             {
#                 'endpoint': 'CronTaskInDolphinScheduler',  # 监控节点，比如主机名，服务名
#                 'metric': self.method + '.failedCount',  # 监控项，数据采集项，指标，比如 cpu.load
#                 'timestamp': self.ts,  # 时间戳
#                 'step': self.step,  # 采集的步距，比如60秒采集一次
#                 'value': push_value,  # 指标的值
#                 'counterType': 'GAUGE',  # 默认
#                 'tags': ''  # tag是为了区分 metric，也用于做筛选，因为有些metric一样，但是tag不一样，就2个监控项
#             }
#         ]
#
#         try:
#             # 把数据提交open-falcon agent的接口，如果本机没有，也可以提交给一个本机可访问的地址，定义好endpoint就可
#             open_falcon_data = json.dumps(payload)
#             logger.info(
#                 'if request send to appServer failed, push data = 1; else, push data = 0,'
#                 '\ntry to push data: {data} \nto open falcon',
#                 data=open_falcon_data)
#             push_data = requests.post(url=self.url_open_falcon, data=open_falcon_data, json=self.headers_json)
#             if push_data.status_code == 200:
#                 logger.info("upload data to falcon successfully")
#             elif push_data.status_code == 404:
#                 logger.info("open-falcon url doesn't exit")
#             elif push_data.status_code == 500:
#                 logger.info("Open-falcon server error, please check Open-falcon status")
#             else:
#                 logger.info("upload data failed, please check")
#         except Exception as e:
#             logger.error("call open falcon push api failed,\n get this exception: {exception}", exception=e)
#
#     # AES加密方法名用于鉴权
#     def encrypt_method_as_token(self):
#         key = '@#$yymmxxkkyuilm'.encode('utf-8')
#         raw = self.method.encode('utf-8')
#         iv = self.get_random_iv()
#         cipher = AES.new(key, AES.MODE_CFB, iv, segment_size=128)
#         encrypted = cipher.encrypt(raw)
#         return base64.urlsafe_b64encode(iv + encrypted).decode('ascii')
#
#     # 获取AES/CFB/NoPadding的16位随机iv偏移量
#     @staticmethod
#     def get_random_iv():
#
#         base_str = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
#         iv_str = ''
#         for i in range(16):
#             iv_str += random.choice(base_str)
#
#         iv = iv_str.encode('utf-8')
#         return iv
