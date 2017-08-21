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
import logging
logger = logging.getLogger("logger")
#基类
class Batch(object):
    #0.类的初始化函数
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName):
        logger.info("开始创建 {classname} 的实例。".format(classname = self.__class__) )
        logger.info("开始定义实例基础属性！")
        self.filesPath = filesPath
        logger.debug("实例定义的xls文件保存位置为： {filesPath} ".format(filesPath = self.filesPath))
        self.csvPath = csvPath
        logger.debug("实例定义的csv文件保存位置为： {csvPath} ".format(csvPath = self.csvPath))
        self.bakPath = bakPath
        logger.debug("实例定义的bak文件保存位置为： {bakPath} ".format(bakPath = self.bakPath))
        self.fileName = fileName
        logger.debug("实例定义的csv文件名为： {fileName} ".format(fileName = self.fileName))
        self.columns = columns
        logger.debug("实例定义的csv文件的表头为： {header} ".format(header = self.columns))
        self.indexName = indexName
        logger.debug("实例定义的索引为： {indexName} ".format(indexName = self.indexName))
        logger.debug("开始整理数据：")
        try:
            df = self.batchProcessing()
        except Exception as e:
            logger.exception("调用批处理失败！")
            raise
        else:
            logger.debug("调用批处理，获得所有xls文件的数据成功！")
        try:
            self.backupData()
        except Exception as e:
            logger.exception("备份源csv文件失败！")
            raise
        else:
            logger.debug("备份源csv文件成功！")
        try:
            self.newData, self.data = self.getNewData(df)
        except Exception as e:
            logger.exception("获取与源csv对比后得到的新数据和合并新数据后的总数据失败！")
            raise
        else:
            logger.debug("获取与源csv对比后得到的新数据和合并新数据后的总数据成功！")
        try:
            self.updata(self.newData)
        except Exception as e:
            logger.exception("更新csv文件失败！")
            raise
        else:
            logger.debug("更新csv文件成功！")
        try:
            logger.debug("开始设置索引！")
            self.data.set_index(indexName)
        except Exception as e:
            logger.exception("设置索引失败！")
            logger.info(" {classname} 实例基础属性定义失败！".format(classname = self.__class__))
            raise
        else:
            logger.debug("设置索引成功！")
            logger.info(" {classname} 实例基础属性定义成功！".format(classname = self.__class__))

    #1.根据 '*.xls' 文件地址获取文件名
    def getName(self):
        try:
            logger.debug("开始获取xls文件地址下所有的文件名！")
            files = os.listdir(self.filesPath) #获取xls文件地址下所有的文件名。
        except Exception as e:
            logger.exception("获取xls文件地址下所有的文件名失败！原因如下：")
            raise
        else:
            logger.debug("获取xls文件地址下所有的文件名成功！")
            for name in files: #使用生成器取出 '*.xls'。
                if name[-3:] == 'xls':
                    yield name
                    logger.debug("返回xls文件名：{name}".format(name = name))

    #2.设置批处理函数，方便继承。
    def batchProcessing(self):
        logger.debug("开始批处理所有xls文件！")
        for i, name in enumerate(self.getName()): #同时取出xls文件名称和索引。
            if i == 0: #第一个文件单独处理。
                df = self.dataWashing(name)
            else: #除第一个文件，其余文件做向前合并处理。
                df2 = self.dataWashing(name)
                df = df.append(df2, ignore_index = True)
        return(df)

    #3，设置每种表格的处理方式，在子类中定义，覆盖父类函数，但批处理使用相同函数名。
    def dataWashing(self, name):
        print('This is a method in class Batch!')
        return(pd.DataFrame())



    #3.备份文件
    def backupData(self):
        logger.debug("开始备份源csv文件！")
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
        logger.debug("开始获取新数据：")
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        try:
            df = pd.read_csv(csvName, encoding = 'gbk', header = 0) #读取源csv，如果文件太大，可能有问题，可能需要分块读取。
        except Exception as e:
            logger.exception("读取源csv文件失败！原因如下：")
            raise
        else:
            data = df.append(data, ignore_index = True) #不修改df，data为新合并后的dataFrame。
            logger.debug("开始检查数据！")
            data = data.applymap(lambda x: self.checkData(x)) #去除数据的特殊格式
            logger.debug("数据检查成功！")
            data = data.drop_duplicates() #去除重复行
            newData = data.iloc[len(df):] #新数据是旧数据之后的数据
            logger.debug("获取新数据成功！")
        return(newData, data) #输出元组（新数据，csv的数据）

    #6.更新本地csv数据
    def updata(self, newData):
        logger.debug("开始更新csv文件！")
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        if len(newData) > 0: #判断有无新数据添加
            with open(csvName, 'a', encoding = 'gbk') as f: #追加模式添加数据
                newData.to_csv(f, header = False, index = False, columns = self.columns, encoding = 'gbk')
            print(" {newData} 被更新加入csv汇总表中！".format(newData = newData))
        else:
            logger.warning("没有新数据需要更新！")

    #7.将datatime格式转为str,int转为float64
    def checkData(self, data):
        try:
            data = data.strftime('%Y-%m-%d')
        except AttributeError as e:
            pass
        return(data)


