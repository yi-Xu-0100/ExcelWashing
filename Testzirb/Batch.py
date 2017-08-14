#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
"""
fileName: Batch.py
Created Time: 2017年08月07日 星期一 20时28分03秒
description:
    This file is consist of many general function or class.
    这个文件包括许多通用函数或者类。
"""

__author__ = 'yi_Xu'

import os
import sys
import shutil
import pandas as pd
import numpy as np
from datetime import datetime

#基类
class Batch(object):
    #0.类的初始化函数
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName):
        #定义参数
        self.filesPath = filesPath #xls文件保存位置
        self.csvPath = csvPath #csv文件保存位置
        self.bakPath = bakPath #bak文件保存位置
        self.fileName = fileName #csv文件名
        self.columns = columns #csv的header
        self.indexName = indexName
        #整理数据
        df = self.batchProcessing() #调用批处理，获得所有xls文件的数据
        self.backupData() #备份源csv文件
        self.newData, self.data = self.getNewData(df) #获取与源csv对比后得到的新数据和合并新数据后的总数据
        self.updata(self.newData) #更新csv文件
        #设置索引
        self.setIndex(self.data)

    #1.根据 '*.xls' 文件地址获取文件名
    def getName(self):
        try:
            files = os.listdir(self.filesPath) #获取xls文件地址下所有的文件名。
        except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
            print("Error:", e)
        else:
            for name in files: #使用生成器取出 '*.xls'。
                if name[-3:] == 'xls':
                    yield name

    #2.设置批处理函数，方便继承。
    def batchProcessing(self):
        try:
            for i, name in enumerate(self.getName()): #同时取出xls文件名称和索引。
                if i == 0: #第一个文件单独处理。
                    df = self.dataWashing(name)
                else: #除第一个文件，其余文件做向前合并处理。
                    df2 = self.dataWashing(name)
                    df = df.append(df2, ignore_index = True)
        except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
            print("Error:", e)
        else:
            print("'*.xls' in %s is successfully concat!" % self.filesPath)
        return(df)

    #3，设置每种表格的处理方式，在子类中定义，覆盖父类函数，但批处理使用相同函数名。
    def dataWashing(self, name):
        print('This is a method in class Batch!')
        return(pd.DataFrame())



    #3.备份文件
    def backupData(self):
        time = datetime.now() #获取时间
        datestamp ='(' + time.strftime("%Y-%m-%d_%H-%M-%S") + ')' #获取时间戳
        newName = self.fileName[:4] + '_bak' + datestamp + self.fileName[4:] #获取新名字
        csvName = os.path.join(self.csvPath, self.fileName) #源文件的位置+名字
        bakName = os.path.join(self.bakPath, newName) #备份文件的位置+名字
        shutil.copy(csvName, bakName) #复制文件
        bakFiles = sorted([name for name in os.listdir(self.bakPath) if name[:4] == self.fileName[:4]]) #获取当前备份过的文件的时间戳，时间较早的在前面
        while (len(bakFiles) > 3): #如果文件数目大于 3 ，则删除到剩余 3 个
            os.remove(os.path.join(self.bakPath,bakFiles.pop(0))) #删除时间戳较早的那一个
        print("bakFiles by now: %s " % bakFiles)

    #5.获得新数据函数
    def getNewData(self, data):
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        try:
            df = pd.read_csv(csvName, encoding = 'gbk', header = 0) #读取源csv，如果文件太大，可能有问题，可能需要分块读取。
        except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
            print("Error:", e)
        else:
            data = df.append(data, ignore_index = True) #不修改df，data为新合并后的dataFrame。
            data = data.applymap(lambda x: self.checkData(x)) #去除数据的特殊格式
            data = data.drop_duplicates() #去除重复行
        newData = data.iloc[len(df):] #新数据是旧数据之后的数据
        return(newData, data) #输出元组（新数据，csv的数据）

    #6.更新本地csv数据
    def updata(self, newData):
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        if len(newData) > 0: #判断有无新数据添加
            with open(csvName, 'a', encoding = 'gbk') as f: #追加模式添加数据
                newData.to_csv(f, header = False, index = False, columns = self.columns, encoding = 'gbk')
            print("%s is successfully Updata!" % newData)
        else:
            print("There is none of new data which need to pull request!")

    #7.设置索引
    def setIndex(self, data):
        data.index = data[self.indexName]
        data.drop(self.indexName, axis = 1, inplace = True)
        data.index = pd.to_datetime(data.index)

    #8.将datatime格式转为str,int转为float64
    def checkData(self, data):
        try:
            data = data.strftime('%Y-%m-%d')
        except Exception as e:
            pass
        return(data)


