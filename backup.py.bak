#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
""" 
FileName: TestThree.py
Author: yi_Xu
Created Time: 03/08/2017 11:09:57 
""" 

""" 
This is a tool to data processing.
这个一个进行数据处理的工具。

""" 

#以下是导入的库
import os
import functools
import sys
import pandas as pd
import numpy as np
from pandas import Series,DataFrame #这个虽然导入这两个库，但是并没有使用，延续使用了pd库的格式。
#导入完成

#以下是通用函数
#1.批处理的装饰器
"""
This is a decorator that combines all the processed data from the specified location, which is used to decorate the merge function. The merge function should reference at least the parameters path and files.
这是一个从指定位置合并所有处理后的数据的装饰器，用于装饰合并函数，合并函数应至少引用变量 path 和 files 。
"""
def batchProcessing(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            def func2(*args, **kw):
                global path, countfile, name, data #定义全局变量，方便引用
                try:
                    if name[-3:] != 'xls':
                        raise NameError(name+' is not xls')
                    countfile = countfile + 1
                    if countfile == 1:
                        data = func(*args, **kw)
                        raise ValueError('first xls')
                    df2 = func(*args, **kw)
                    data = data.append(df2, ignore_index = True)
                except ValueError:
                    print("find the first xls! wait a moment……")
                except Exception as e:
                    print("Error in line: %s" % (sys._getframe().f_lineno + 1)) #输出行号
                    print("Error:", e)
                else:
                    print("%s is successfully concat!" % name)
                finally:
                    return(data)
            return func2(*args, **kw)
        return wrapper

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

#更新函数
def updata(data, fileName, columns):
    try:
        df = pd.read_csv(fileName, encoding = 'gbk',header = 0)
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
            print("Error in line: %s" % (sys._getframe().f_lineno + 1))
            print("==>" + fileName + "***更新*** 异常，请检查！")
            print("Error:", e)
    else:
        print("%s is successfully Updata!" % fileName)
        return(df)

#通用函数完成，可新建文件保存，以后以导入自定义库方式引用函数

#以下为jsyb的各类处理
#1.处理jsyb的批处理函数
"""
This is a functon for get data from form which from xls, and it will be compiled after the data summary.
这是一个从xls中获取数据的函数，会将整理后的数据汇总。
"""
@batchProcessing
def jsybtiqu(path, name):
    try: #获取固定数据
        fileName = os.path.join(path, name)
        jsyb = pd.read_excel(fileName, '客户交易结算月报', header=None)
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
            a = Series(jsyb.iloc[4,7], index=crjmxb.index)
            crjmxb['交易月份'] = a
            crjmxb.index = range(len(crjmxb.index))
            df = pd.merge(df1, crjmxb, how='outer', on='交易月份', left_index=True, right_index=True)
        else:
            df = df1
    except Exception as e:
        print("Error in line: %s" % (sys._getframe().f_lineno + 1))
        print(fileName + "==>'客户交易结算月报' ***读取*** 异常，请检查！")
        print("Error:", e)
    else:
        print("%s is successfully read!" % name)
        return(df)

def jsybsort():
    try:
        global path, countfile, name
        path = input("请输入jsyb表格所在位置：")
        files = os.listdir(path)
        columns = ['交易日期','交易月份','上月结存','当月存取合计','当月盈亏','当月总权利金','当月手续费','当月结存','浮动盈亏','客户权益','实有货币资金','非货币充抵金额','货币充抵金额','冻结资金','保证金占用','可用资金','风险度','追加保证 金','入金','出金','方式','摘要']
        countfile = 0
        for name in files:
            try:
                df = jsybtiqu(path, name)
            except Exception as e:
                print("Error in line: %s" % (sys._getframe().f_lineno + 1))
                print("lineError:", e)
                continue
        countfile = 0
        backupPath = input("请输入jsyb表格保存位置：")
        fileName = os.path.join(backupPath,'jsyb.csv')
        df = updata(df, fileName, columns)
    except Exception as e:
        print("Error in line: %s" % (sys._getframe().f_lineno + 1))
        print("Error:", e)
    else:
        print("jsyb is successfully sorted!")
        return(df)

if __name__ == '__main__':
    df = jsybsort()
    print(df)
    
    
    
    
    
    
    
    
    
    
    
    
