#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
""" 
File Name: new.py
Author: yi_Xu
Created Time: 2017年08月03日 星期四 21时19分54秒
""" 

""" 


"""
def Read_jsyb_pzhz(pathtiqu, name):
    try:
        filename = os.path.join(pathtiqu, name)
        pzhz = pd.read_excel(filename,'品种汇总',header=None)
        pzhzb = pzhz.iloc[10:-1,0:6]
        pzhzb.columns = ['交易日期','品种','手数','成交额','手续费','平仓盈亏']
        a = pd.Series(pzhz.iloc[2,7],index=pzhzb.index)
        pzhzb['交易月份'] = a
        pzhzb.index = range(len(pzhzb.index))
    except Exception as e:
        print("==>'客户交易结算月报-->品种汇总' ***读取*** 异常，请检查！")
        print("error:", e)
    else:
        print("%s is successfully read!" %name)
        return(pzhzb)

def Append_jsyb_pzhz(pathtiqu):
    try:
        files = os.listdir(pathtiqu)
        n = 0
        for name in files:
            if name[-3:] != 'xls':
                continue
            n = n + 1
            if n == 1:
                df1 = Read_jsyb_pzhz(pathtiqu, name)
                continue
            df2 = Read_jsyb_pzhz(pathtiqu, name)
            #df = pd.concat([df, df2])
            df = df1.append(df2, ignore_index=True).drop_duplicates()
    except Exception as e:
        print("==>'客户交易结算月报-->品种汇总' ***合并*** 异常，请检查！")
        print("Error:", e)
    else:
        print("%s is successfully concat!" %name)
        return(df)

def Update_jsyb_pzhz(file_pzhz, pathtiqu):
    try:
        df1 = pd.read_csv(file_pzhz, encoding='gbk')
        df2 = Append_jsyb_pzhz(pathtiqu)
        #df = pd.concat([df1, df2]).drop_duplicates()
        df = df1.append(df2, ignore_index=True).drop_duplicates()
    except Exception as e:
        print("==>'客户交易结算月报-->品种汇总' ***更新*** 异常，请检查！")
        print("Error:", e)
    else:
        print("%s is successfully Update!" %file_pzhz)
        return(df)
