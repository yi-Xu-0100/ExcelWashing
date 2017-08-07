#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
""" 
File Name: new3.py
Author: yi_Xu
Created Time: 2017年08月03日 星期四 21时23分25秒
""" 

""" 


""" 
def jsyb_cjmx(pathtiqu, name):
    try:
        filename = os.path.join(pathtiqu, name)
        cjmx = pd.read_excel(filename,'成交明细',header=None)
        cjmxb = cjmx.iloc[10:-1,:]
        cjmxb.columns = ["交易日期","合约","成交序号","成交时间","买/卖","投机/套保","成交价","手数","成交额","开/平","手续费","平仓盈亏","实际成交日期"]
        cjmxb.index = range(len(cjmxb.index))
        cjmxb.index = cjmxb['交易日期']
        cjmxb = cjmxb.iloc[:,1:]
        a = pd.Series(cjmx.iloc[2,7],index=cjmxb.index)
        cjmxb['交易月份'] = a
        cjmxb.index = pd.to_datetime(cjmxb.index)
    except Exception as e:
        print("“客户交易结算月报：《成交明细》 ” 数据 ***读取*** 异常，请检查！")
        print("error:", e)
    return(cjmxb)
