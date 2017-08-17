#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
"""
fileName: test.py
Created Time: 2017年08月17日 星期四 20时38分41秒
description:
"""

__author__ = 'yi_Xu'

import logging
logger = logging.getLogger("logger")

def test():
    logger.debug("测试debug")
    logger.info("测试info")
    logger.warn("测试warn")
    logger.error("测试error")
    logger.critical("测试critical")
