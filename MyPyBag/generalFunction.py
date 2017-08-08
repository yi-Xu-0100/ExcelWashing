#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
"""
fileName: generalFunction.py
Created Time: 2017年08月07日 星期一 20时28分03秒
description:
    This file is consist of many general function or class.
    这个文件包括许多通用函数或者类。
"""

__author__ = 'yi_Xu'

import os
import functools
import sys
import pandas as pd

#1.批处理的装饰器
class Batch(object):
    def __init__(self, filesPath, csvPath, bakPath):
        self.filesPath = filesPath
        self.csvPath = csvPath
        self.bakPath = bakPath

    def batchProcessing(self):
        try:
            for i, name in enumerate(getName(self.filesPath)):
                if i == 0:
                    df = self.dataWashing(name)
                else:
                    df2 = self.dataWashing(name)
                    df = df.append(df2, ignore_index = True)
        except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
            print("Error:", e)
        else:
            print("'*.xls' in %s is successfully concat!" % self.filesPath)
#            finally:
#                return(data)
        return(df)

    def dataWashing(self, name):
        print('This is a method in class Batch!')
        return(pd.DataFrame())
#2.去除重复行数据
def dataDropDup(columns, fileName):
    df2 = pd.read_csv(fileName, encoding = 'gbk', header = 0)
    df2 = df2.drop_duplicates()
    backupData(fileName)
    df2.to_csv(fileName, encoding = 'gbk', columns = columns, index = False)
    return(df2)

#3.备份文件
def backupData(fileName):
    try:
        os.rename(fileName, fileName[:-4] + '_bak' + fileName[-4:])
    except Exception as e:
        print("Error in line: %s" % (sys._getframe().f_lineno + 1))
        print("Error:", e)
        os.remove(fileName[:-4] + '_bak' + fileName[-4:])
        os.rename(fileName, fileName[:-4] + '_bak' + fileName[-4:])


def getName(filesPath):
    try:
        files = os.listdir(filesPath)
    except Exception as e:
        print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
        print("Error:", e)
    else:
        for name in files:
            if name[-3:] == 'xls':
                yield name
        
#更新函数
def updata(data, fileName, columns):
    try:
        df = pd.read_csv(fileName, encoding = 'gbk', header = 0)
    except Exception as e:
        print("Error in line: %s" % (sys._getframe().f_lineno + 1))
        print("Error:", e)
    else:
        data = df.append(data,ignore_index = True)
        backupData(fileName)
    try:
        data.to_csv(fileName, encoding = 'gbk', columns = columns, index = False)
        df = dataDropDup(columns, fileName)
    except Exception as e:
        print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
        print("==>" + fileName + "***更新*** 异常，请检查！")
        print("Error:", e)
    else:
        print("%s is successfully Updata!" % fileName)
        return(df)

