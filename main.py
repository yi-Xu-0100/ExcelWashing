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
#导入完成

if __name__ == '__main__':
#1.jsyb的处理
    try:
        filesPath = './xls'#input("请输入 jsyb 表格所在位置：")
        columns = ['交易日期','交易月份','上月结存','当月存取合计','当月盈亏','当月总权利金','当月手续费','当月结存','浮动盈亏','客户权益','实有货币资金','非货币充抵金额','货币充抵金额','冻结资金','保证金占用','可用资金','风险度','追加保证金','入金','出金','方式','摘要'] #设置列的排列应该与csv表格一致
        csvPath = './csv'#input("请输入 'jsyb.csv' 表格位置：")
        bakPath = './bak'#input("请输入 'jsyb_bak.csv'表格位置：") #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        jsyb = gf.Jsyb(filesPath, csvPath, bakPath, 'jsyb.csv', columns) #初始化一个jsyb类，建立实例
        print(jsyb.money)
    except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './main.py')) #输出行号
            print("Error:", e)
    
    
    
    
    
    
    
