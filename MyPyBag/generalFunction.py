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
import sys
import shutil
import pandas as pd
import numpy as np
from datetime import datetime

#基类
class Batch(object):
    #0.类的初始化函数
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns): 
        self.filesPath = filesPath #xls文件保存位置
        self.csvPath = csvPath #csv文件保存位置
        self.bakPath = bakPath #bak文件保存位置
        self.fileName = fileName #csv文件名
        self.columns = columns #csv的header

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
        datestamp ='(' + time.strftime("%y-%m-%d_%H:%M:%S") + ')' #获取时间戳
        newName = self.fileName[:4] + '_bak' + datestamp + self.fileName[4:] #获取新名字
        csvName = os.path.join(self.csvPath, self.fileName) #源文件的位置+名字
        bakName = os.path.join(self.bakPath, newName) #备份文件的位置+名字
        shutil.copyfile(csvName, bakName) #复制文件
        bakFiles = sorted([name for name in os.listdir(self.bakPath) if name[:4] == self.fileName[:4]]) #获取当前备份过的文件的时间戳，时间较早的在前面
        while (len(bakFiles) > 3): #如果文件数目大于 3 ，则删除到剩余 3 个
            os.remove(os.path.join(self.bakPath,bakFiles.pop(0))) #删除时间戳较早的那一个
        print("bakFiles by now: %s " % bakFiles)

    #获得新数据函数
    def getNewData(self, data):
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        try:
            df = pd.read_csv(csvName, encoding = 'gbk', header = 0) #读取源csv，如果文件太大，可能有问题，可能需要分块读取。
        except Exception as e:
            print("Error in line: %s , file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/generalFunction.py')) #输出行号和文件名
            print("Error:", e)
        else:
            data = df.append(data, ignore_index = True) #不修改df，data为新合并后的dataFrame。
            data = data.drop_duplicates() #去除重复行
        newData = data.iloc[len(df):] #新数据是旧数据之后的数据
        return(newData, data) #输出元组（新数据，csv的数据）

    #5.更新本地csv数据
    def updata(self, newData):
        csvName = os.path.join(self.csvPath, self.fileName) #获取csv地址+名字
        if len(newData) > 0: #判断有无新数据添加
            with open(csvName, 'a', encoding = 'gbk') as f: #追加模式添加数据
                newData.to_csv(f, header = False, index = False, columns = self.columns)
            print("%s is successfully Updata!" % newData)
        else:
            print("There is none of new data which need to pull request!")

#设置jsyb类，继承于Batch
class Jsyb(Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns): 
        super().__init__(filesPath, csvPath, bakPath, fileName, columns) #初始化各个的值
        df = self.batchProcessing() #调用批处理，获得所有xls文件的数据
        self.backupData() #备份源csv文件
        self.newData, self.data = self.getNewData(df) #获取与源csv对比后得到的新数据和合并新数据后的总数据
        self.updata(self.newData) #更新csv文件
        self.money = self.data['当月结存']
    #复写dataWashing函数，定义表格处理方式
    def dataWashing(self,name):
        try: #获取固定数据
            pName = os.path.join(self.filesPath, name)
            jsyb = pd.read_excel(pName, '客户交易结算月报', header=None)
            df1 = pd.DataFrame()
            a1 = pd.Series(jsyb.iloc[4,7], index=jsyb.index[:1])
            df1['交易月份'] = a1
            a2 = pd.Series(jsyb.iloc[9,2], index=jsyb.index[:1])
            df1['上月结存'] = a2
            a3 = pd.Series(jsyb.iloc[10,2], index=jsyb.index[:1])
            df1['当月存取合计'] = a3
            a4 = pd.Series(jsyb.iloc[11,2], index=jsyb.index[:1])
            df1['当月盈亏'] = a4
            a5 = pd.Series(jsyb.iloc[12,2], index=jsyb.index[:1])
            df1['当月总权利金'] = a5
            a6 = pd.Series(jsyb.iloc[13,2], index=jsyb.index[:1])
            df1['当月手续费'] = a6
            a7 = pd.Series(jsyb.iloc[14,2], index=jsyb.index[:1])
            df1['当月结存'] = a7
            a8 = pd.Series(jsyb.iloc[15,2], index=jsyb.index[:1])
            df1['浮动盈亏'] = a8
            b1 = pd.Series(jsyb.iloc[9,7], index=jsyb.index[:1])
            df1['客户权益'] = b1
            b2 = pd.Series(jsyb.iloc[10,7], index=jsyb.index[:1])
            df1['实有货币资金'] = b2
            b3 = pd.Series(jsyb.iloc[11,7], index=jsyb.index[:1])
            df1['非货币充抵金额'] = b3
            b4 = pd.Series(jsyb.iloc[12,7], index=jsyb.index[:1])
            df1['货币充抵金额'] = b4
            b5 = pd.Series(jsyb.iloc[13,7], index=jsyb.index[:1])
            if type(jsyb.iloc[13,7]) != type(1): #此处的冻结资金有异常数据为"--"，加以判断并设为NaN。
                b5 = np.repeat(np.nan, len(b5))
            df1['冻结资金'] = b5
            b6 = pd.Series(jsyb.iloc[14,7], index=jsyb.index[:1])
            df1['保证金占用'] = b6
            b7 = pd.Series(jsyb.iloc[15,7], index=jsyb.index[:1])
            df1['可用资金'] = b7
            b8 = pd.Series(jsyb.iloc[16,7], index=jsyb.index[:1])
            df1['风险度'] = b8
            b9 = pd.Series(jsyb.iloc[17,7], index=jsyb.index[:1])
            df1['追加保证金'] = b9
            crjmx = jsyb.iloc[21:-3,:]
            if len(crjmx.index) != 0: #以下数据在部分表格中没有，加以判断以选择是否忽略处理。
                crjmx.columns = ['交易日期', 1, '入金', 3, '出金', 5, '方式', 7, '摘要']
                crjmx = crjmx[['交易日期', '入金', '出金', '方式', '摘要']]
                crjmxb = crjmx.copy()
                a = pd.Series(jsyb.iloc[4,7], index=crjmxb.index)
                crjmxb['交易月份'] = a
                crjmxb.index = range(len(crjmxb.index))
                df = pd.merge(df1, crjmxb, how='outer', on='交易月份', left_index=True, right_index=True)
            else: #如果上述数据没有，只需要整理基本数据。
                df = df1
        except Exception as e: #如果有异常，捕捉并报错。
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './MyPyBag/jsybFunction.py')) #显示报错的在文件的多少行和文件名。
            print(pName + "==>'客户交易结算月报' ***读取*** 异常，请检查！") #提示报错信息。
            print("Error:", e) #提示系统报错内容。
        else:
            print("%s is successfully read!" % name) #如果没有错误，显示读取成功。
            return(df)
