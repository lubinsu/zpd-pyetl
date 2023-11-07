#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 8/26/2021 03:19 PM 
# @Author : lubinsu 
# @File : zpd_logging.py
# @desc : 自定义日志工具

from zpd_logging.handler.logger_handler_to_mysql import *


class JobLoggerAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs["extra"] = self.extra
        return msg, kwargs


