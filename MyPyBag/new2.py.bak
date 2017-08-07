#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
""" 
File Name: new2.py
Author: yi_Xu
Created Time: 2017年08月03日 星期四 21时23分00秒
""" 

""" 


""" 
def jsyb_ccmx(pathtiqu, name):
    try:
        filename = os.path.join(pathtiqu, name)
        ccmx = pd.read_excel(filename,'持仓明细',header=None)
        ccmxb = ccmx.iloc[10:-1,:]
        ccmxb.columns = ['合约','成交序号','买持仓','买入价','卖持仓','卖出价','昨结算价','今结算价','持仓盈亏',"投机/套保","交易编码","交易日期"]
        ccmxb.index = range(len(ccmxb.index))
        ccmxb.index = ccmxb['交易日期']
        ccmxb = ccmxb.iloc[:,:-1]
        a = pd.Series(ccmx.iloc[2,7],index=ccmxb.index)
        ccmxb['交易月份'] = a
        ccmxb.index = pd.to_datetime(ccmxb.index)
    except Exception as e:
        print("“客户交易结算月报：《持仓明细》 ” 数据 ***读取*** 异常，请检查！")
        print("error:", e)
    return(ccmxb)
