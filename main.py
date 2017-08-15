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

import os
import sys
from Testzirb import jsyb
from Testzirb import jsrb
import logging
import logging.config
import yaml
def setup_logging(default_path = "./logconfig.yaml",default_level = logging.INFO,env_key = "LOG_CFG"):
    path = default_path
    value = os.getenv(env_key,None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path,"r") as f:
            config = yaml.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level = default_level)
#导入完成


if __name__ == '__main__':
    setup_logging(default_path = "./logconfig.yaml")
    logger = logging.getLogger("logger")
    logger.info("本次日志记录开始：")
#1.1 jsyb 的处理
    try:
        filesPath = r'./xls/jsyb/'#'F:\CZJY\Test\Test02\xls\jsyb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['交易月份','上月结存','当月存取合计','当月盈亏','当月总权利金','当月手续费','当月结存','浮动盈亏','客户权益','实有货币资金','非货币充抵金额','货币充抵金额','冻结资金','保证金占用','可用资金','风险度','追加保证金'] #设置列的排列应该与csv表格一致
        csvPath = r'./csv/jsybcsv/'#'F:\CZJY\Test\Test02\csv\jsybcsv' #input("请输入 'jsyb.csv' 表格位置：")
        bakPath = r'./bak/bakjsyb/'#'F:\CZJY\Test\Test02\bak\bakjsyb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        indexName = '交易月份'
        jsyb_1705 = jsyb.Jsyb(filesPath, csvPath, bakPath, 'jsyb.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsyb_1705.data)
    except Exception as e:
        logger.exception(' <class jsyb.Jsyb> 实例化失败，数据无法获取！')

#1.2 jsyb_crjmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsyb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['发生日期','入金','出金','方式','摘要','交易月份'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsybcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsyb' #input("请输入 'jsyb_bak.csv'表格位置：") #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易月份'
        jsyb_crjmx_1705 = jsyb.Jsyb_crjmx(filesPath, csvPath, bakPath, 'jsyb_crjmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsyb_crjmx_1705.data)
    except Exception as e:
        logger.exception(' <class jsyb.Jsyb_crjmx> 实例化失败，数据无法获取！')

#1.3 jsyb_pzhz 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsyb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['交易日期','品种','手数','成交额','手续费','平仓盈亏','交易月份'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsybcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsyb' #input("请输入 'jsyb_bak.csv'表格位置：") #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易月份'
        jsyb_pzhz_1705 = jsyb.Jsyb_pzhz(filesPath, csvPath, bakPath, 'jsyb_pzhz.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsyb_pzhz_1705.data)
    except Exception as e:
        logger.exception(' <class jsyb.Jsyb_pzhz> 实例化失败，数据无法获取！')

#1.4 jsyb_ccmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsyb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['合约','成交序号','买持仓','买入价','卖持仓','卖出价','昨结算价','今结算价','持仓盈亏',"投机/套保","交易编码","实际成交日期",'交易月份'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsybcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsyb' #input("请输入 'jsyb_bak.csv'表格位置：") #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易月份'
        jsyb_ccmx_1705 = jsyb.Jsyb_ccmx(filesPath, csvPath, bakPath, 'jsyb_ccmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsyb_ccmx_1705.data)
    except Exception as e:
        logger.exception(' <class jsyb.Jsyb_ccmx> 实例化失败，数据无法获取！')

#1.5 jsyb_cjmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsyb'  #input("请输入 jsyb 表格所在位置：")
        columns = ["交易日期","合约","成交序号","成交时间","买/卖","投机/套保","成交价","手数","成交额","开/平","手续费","平仓盈亏","实际成交日期",'交易月份'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsybcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsyb' #input("请输入 'jsyb_bak.csv'表格位置：") #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易月份'
        jsyb_cjmx_1705 = jsyb.Jsyb_cjmx(filesPath, csvPath, bakPath, 'jsyb_cjmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsyb_cjmx_1705.data)
    except Exception as e:
        logger.exception(' <class jsyb.Jsyb_cjmx> 实例化失败，数据无法获取！')


#2.1 jsrb 的处理
    try:
        filesPath = r'./xls/jsrb/'#'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['交易日期','上日结存','当日存取合计','当日盈亏','当日总权利金','当日手续费','当日结存','客户权益','实有货币资金','非货币充抵金额','货币充抵金额','冻结资金','保证金占用','可用资金','风险度','追加保证金'] #设置列的排列应该与csv表格一致
        csvPath = r'./csv/jsrbcsv/'#'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        bakPath = r'./bak/bakjsrb/'#'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        indexName = '交易日期'
        jsrb_170810 = jsrb.Jsrb(filesPath, csvPath, bakPath, 'jsrb.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb> 实例化失败，数据无法获取！')

#2.2 jsrb_crjmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['发生日期', '入金', '出金', '方式', '摘要','交易日期'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易日期'
        jsrb_crjmx_170810 = jsrb.Jsrb_crjmx(filesPath, csvPath, bakPath, 'jsrb_crjmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_crjmx_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb_crjmx> 实例化失败，数据无法获取！')
#2.3 jsrb_pzhz 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['品种','手数','成交额','手续费','平仓盈亏','交易日期'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易日期'
        jsrb_pzhz_170810 = jsrb.Jsrb_pzhz(filesPath, csvPath, bakPath, 'jsrb_pzhz.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_pzhz_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb_pzhz> 实例化失败，数据无法获取！')

#2.4 jsrb_cjmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ["合约","成交序号","成交时间","买/卖","投机/套保","成交价","手数","成交额","开/平","手续费","平仓盈亏","实际成交日期",'交易日期'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        indexName = '交易日期'
        jsrb_cjmx_170810 = jsrb.Jsrb_cjmx(filesPath, csvPath, bakPath, 'jsrb_cjmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_cjmx_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb_cjmx> 实例化失败，数据无法获取！')

#2.5 jsrb_pcmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ["合约","成交序号","买/卖","成交价","开仓价","手数","昨结算价","平仓盈亏","原成交序号","实际成交日期",'交易日期'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        indexName = '交易日期'
        jsrb_pcmx_170810 = jsrb.Jsrb_pcmx(filesPath, csvPath, bakPath, 'jsrb_pcmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_pcmx_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb_pcmx> 实例化失败，数据无法获取！')

#2.6 jsrb_ccmx 的处理
    try:
        #filesPath = r'F:\CZJY\Test\Test02\xls\jsrb'  #input("请输入 jsyb 表格所在位置：")
        columns = ['合约','成交序号','买持仓','买入价','卖持仓','卖出价','昨结算价','今结算价','持仓盈亏',"投机/套保","交易编码","实际成交日期",'交易日期'] #设置列的排列应该与csv表格一致
        #csvPath = r'F:\CZJY\Test\Test02\csv\jsrbcsv' #input("请输入 'jsyb.csv' 表格位置：")
        #bakPath = r'F:\CZJY\Test\Test02\bak\bakjsrb' #input("请输入 'jsyb_bak.csv'表格位置：")  #保存方式，文件名后添加时间戳，仅保存两个，每次删掉时间更早的一份。
        #indexName = '交易日期'
        jsrb_ccmx_170810 = jsrb.Jsrb_ccmx(filesPath, csvPath, bakPath, 'jsrb_ccmx.csv', columns, indexName) #初始化一个jsyb类，建立实例
        print(jsrb_ccmx_170810.data)
    except Exception as e:
        logger.exception(' <class jsrb.Jsrb_ccmx> 实例化失败，数据无法获取！')
    logger.info("本次日志记录完成！")




