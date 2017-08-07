#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
"""
fileName: main.py
Created Time: 2017年08月07日 星期一 19时59分36秒
description:
    This is a tool to data processing.
    这个一个进行数据处理的工具。
"""

__author__ = 'yi_Xu'

#以下是导入的库
import os
import functools
import sys
import pandas as pd
import numpy as np
from pandas import Series,DataFrame #这个虽然导入这两个库，但是并没有使用，延续使用了pd库的格式。
from MyPyBag import generalFunction as gf
from MyPyBag import jsybFunction as jf
#导入完成

if __name__ == '__main__':
    try:
        global path, name, data, countfile
        path = input("请输入jsyb表格所在位置：")
        files = os.listdir(path)
        columns = ['交易日期','交易月份','上月结存','当月存取合计','当月盈亏','当月总权利金','当月手续费','当月结存','浮动盈亏','客户权益','实有货币资金','非货币充抵金额','货币充抵金额','冻结资金','保证金占用','可用资金','风险度','追加保证 金','入金','出金','方式','摘要']
        countfile = 0
        for name in files:
            try:
                df = jf.jsybtiqu(path, name)
            except Exception as e:
                print("Error in line: %s" % (sys._getframe().f_lineno + 1))
                print("lineError:", e)
                continue
        countfile = 0
        backupPath = input("请输入jsyb表格保存位置：")
        fileName = os.path.join(backupPath,'jsyb.csv')
        df = gf.updata(df, fileName, columns)
    except Exception as e:
        print("Error in line: %s" % (sys._getframe().f_lineno + 1))
        print("Error:", e)
    else:
        print("jsyb is successfully sorted!")
    print(df)
