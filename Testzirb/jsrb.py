#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
"""
fileName: jsrb.py
Created Time: 2017年08月14日 星期一 17时43分57秒
description:
"""

__author__ = 'yi_Xu'


import os
import sys
import pandas as pd
import numpy as np
from Testzirb import Batch as bh



# 设置 jsrb 类，继承于 Batch
class Jsrb(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsrb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName): 
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值
        self.totalcapital = self.data['当日结存']
        self.dayhandlingfee = self.data['当日手续费']
        self.dayprofitandloss = self.data['当日盈亏']
        self.daycash = self.data['可用资金']
        self.dayriskdegree = self.data['风险度']

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try: #获取固定数据
            jsrbName = os.path.join(self.filesPath, name)
            jsrb = pd.read_excel(jsrbName, '客户交易结算日报', header=None)
            df = pd.DataFrame()
            a1 = pd.Series(jsrb.iloc[4,7], index=jsrb.index[:1])
            df['交易日期'] = a1
            a2 = pd.Series(jsrb.iloc[9,2], index=jsrb.index[:1])
            df['上日结存'] = a2
            a3 = pd.Series(jsrb.iloc[10,2], index=jsrb.index[:1])
            df['当日存取合计'] = a3
            a4 = pd.Series(jsrb.iloc[11,2], index=jsrb.index[:1])
            df['当日盈亏'] = a4
            a5 = pd.Series(jsrb.iloc[12,2], index=jsrb.index[:1])
            df['当日总权利金'] = a5
            a6 = pd.Series(jsrb.iloc[13,2], index=jsrb.index[:1])
            df['当日手续费'] = a6
            a7 = pd.Series(jsrb.iloc[14,2], index=jsrb.index[:1])
            df['当日结存'] = a7
            #a8 = pd.Series(jsrb.iloc[15,2], index=jsrb.index[:1])
            #df1['浮动盈亏'] = a8
            b1 = pd.Series(jsrb.iloc[9,7], index=jsrb.index[:1])
            df['客户权益'] = b1
            b2 = pd.Series(jsrb.iloc[10,7], index=jsrb.index[:1])
            df['实有货币资金'] = b2
            b3 = pd.Series(jsrb.iloc[11,7], index=jsrb.index[:1])
            df['非货币充抵金额'] = b3
            b4 = pd.Series(jsrb.iloc[12,7], index=jsrb.index[:1])
            df['货币充抵金额'] = b4
            b5 = pd.Series(jsrb.iloc[13,7], index=jsrb.index[:1])
            if type(jsrb.iloc[13,7]) != type(1):  #此处的冻结资金有异常数据为"--"，加以判断并设为NaN。
                b5 = np.repeat(np.nan, len(b5))
            df['冻结资金'] = b5
            b6 = pd.Series(jsrb.iloc[14,7], index=jsrb.index[:1])
            df['保证金占用'] = b6
            b7 = pd.Series(jsrb.iloc[15,7], index=jsrb.index[:1])
            df['可用资金'] = b7
            b8 = pd.Series(jsrb.iloc[16,7], index=jsrb.index[:1])
            df['风险度'] = b8
            b9 = pd.Series(jsrb.iloc[17,7], index=jsrb.index[:1])
            df['追加保证金'] = b9
            df.index = range(len(df.index))
        except Exception as e: #如果有异常，捕捉并报错。
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print(jsrbName + "==>'客户交易结算日报' ***读取*** 异常，请检查！") #提示报错信息。
            print("Error:", e) #提示系统报错内容。
        else:
            print("%s is successfully read!" % name) #如果没有错误，显示读取成功。
            return(df)

#设置 Jsrb_crjmx 类，继承于 Batch
class Jsrb_crjmx(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName): 
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值
        self.intogold = self.data['入金']
        self.gold = self.data['出金']

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try:
            jsrbName = os.path.join(self.filesPath, name)
            jsrb = pd.read_excel(jsrbName, '客户交易结算日报', header=None)
            crjmx = jsrb.iloc[21:-3,:]
            if len(crjmx.index) != 0: #以下数据在部分表格中没有，加以判断以选择是否忽略处理。
                crjmx.columns = ['发生日期', 1, '入金', 3, '出金', 5, '方式', 7, '摘要']
                crjmx = crjmx[['发生日期', '入金', '出金', '方式', '摘要']]
                crjmxb = crjmx.copy()
                a = pd.Series(jsrb.iloc[4,7], index=crjmxb.index)
                crjmxb['交易日期'] = a
                crjmxb.index = range(len(crjmxb.index))
            else: #如果上述数据没有，则跳过。
                pass
        except Exception as e:
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print(jsrbName + "==>'客户交易结算日报-->出入金明细' ***读取*** 异常，请检查！")
            print("error:", e)
        else:
            print("%s is successfully read!" % name)
            return(crjmxb)

#设置 Jsrb_pzhz 类，继承于 Batch
class Jsrb_pzhz(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName): 
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try:
            pzhzName = os.path.join(self.filesPath, name)
            pzhz = pd.read_excel(pzhzName, '品种汇总', header=None)
            pzhzb = pd.DataFrame()
            pzhzb = pzhz.iloc[10:-1,0:6]
            pzhzb.columns = ['品种','手数','成交额',3,'手续费','平仓盈亏']
            pzhzb = pzhzb[['品种','手数','成交额','手续费','平仓盈亏']]
            a = pd.Series(pzhz.iloc[2,7],index=pzhzb.index)
            pzhzb['交易日期'] = a
            pzhzb.index = range(len(pzhzb.index))
        except Exception as e:
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print(pzhzName + "==>'客户交易结算日报-->品种汇总' ***读取*** 异常，请检查！")
            print("error:", e)
        else:
            print("%s is successfully read!" % name)
            return(pzhzb)

#设置 Jsrb_cjmx 类，继承于 Batch
class Jsrb_cjmx(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName):
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值
        self.contract = self.data["合约"]
        self.transactionprice = self.data["成交价"]
        self.positions = self.data["手数"]
        self.turnover = self.data["成交额"]
        self.handlingfee = self.data["手续费"]
        self.flatprofitandloss = self.data["平仓盈亏"]

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try:
            cjmxName = os.path.join(self.filesPath, name)
            cjmx = pd.read_excel(cjmxName, '成交明细', header=None)
            cjmxrb = cjmx.iloc[10:-1,:]
            cjmxrb.columns = ["合约","成交序号","成交时间","买/卖","投机/套保","成交价","手数","成交额","开/平","手续费","平仓盈亏","实际成交日期"]
            cjmxb = cjmxrb.copy()
            a = pd.Series(cjmx.iloc[2,7],index=cjmxb.index)
            cjmxb['交易日期'] = a
            cjmxb.index = range(len(cjmxb.index))
        except Exception as e:
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print("==>" + cjmxName + "'客户交易结算日报-->成交明细' ***读取*** 异常，请检查！")
            print("error:", e)
        else:
            print("%s is successfully read!" % name)
            return(cjmxb)

#设置 Jsrb_pcmx 类，继承于 Batch
class Jsrb_pcmx(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName): 
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值
        self.contract = self.data["合约"]
        self.transactionprice = self.data["成交价"]
        self.openingprice = self.data["开仓价"]
        self.positions = self.data["手数"]
        self.yesterdaysetprice = self.data["昨结算价"]
        self.flatprofitandloss = self.data["平仓盈亏"]

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try:
            pcmxName = os.path.join(self.filesPath, name)
            pcmx = pd.read_excel(pcmxName, '平仓明细', header=None)
            pcmxrb = pcmx.iloc[10:-1,:]
            pcmxrb.columns = ["合约","成交序号","买/卖","成交价","开仓价","手数","昨结算价","平仓盈亏","原成交序号","实际成交日期"]
            pcmxb = pcmxrb.copy()
            a = pd.Series(pcmx.iloc[2,7],index=pcmxb.index)
            pcmxb['交易日期'] = a
            pcmxb.index = range(len(pcmxb.index))
        except Exception as e:
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print("==>" + pcmxName + "'客户交易结算日报-->平仓明细' ***读取*** 异常，请检查！")
            print("error:", e)
        else:
            print("%s is successfully read!" % name)
            return(pcmxb)

#设置 Jsrb_ccmx 类，继承于 Batch
class Jsrb_ccmx(bh.Batch):
    """
        getName, batchProcessing, getNewData, updata 是通用函数，不予复写
    """
    #1.复写__init__函数，定义jsyb类的属性
    def __init__(self, filesPath, csvPath, bakPath, fileName, columns, indexName):
        super().__init__(filesPath, csvPath, bakPath, fileName, columns, indexName) #初始化各个的值
        self.contract = self.data["合约"]
        self.buypositions = self.data["买持仓"]
        self.buyprice = self.data["买入价"]
        self.sellpositions = self.data["卖持仓"]
        self.sellprice = self.data["卖出价"]
        self.yesterdaysetprice = self.data["昨结算价"]
        self.todaysetprice = self.data["今结算价"]
        self.profitandloss = self.data["持仓盈亏"]

    #2.复写dataWashing函数，定义表格处理方式
    def dataWashing(self, name):
        try:
            ccmxName = os.path.join(self.filesPath, name)
            ccmx = pd.read_excel(ccmxName, '持仓明细', header=None)
            ccmxrb = ccmx.iloc[10:-1,:]
            ccmxrb.columns = ['合约','成交序号','买持仓','买入价','卖持仓','卖出价','昨结算价','今结算价','持仓盈亏',"投机/套保","交易编码","实际成交日期"]
            ccmxb = ccmxrb.copy()
            a = pd.Series(ccmx.iloc[2,7],index=ccmxb.index)
            ccmxb['交易日期'] = a
            ccmxb.index = range(len(ccmxb.index))
        except Exception as e
            print("Error in line: %s ，file name : %s" % (sys._getframe().f_lineno + 1, './Testzirb/jsrb.py')) #显示报错的在文件的多少行和文件名。
            print("==>" + ccmxName + "'客户交易结算日报-->持仓明细' ***读取*** 异常，请检查！")
            print("error:", e)
        else:
            print("%s is successfully read!" % name)
            return(ccmxb)
