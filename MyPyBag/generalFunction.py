#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
""" 
File Name: generalFunction.py
Author: yi_Xu
Created Time: 2017年08月04日 星期五 12时50分07秒
""" 

""" 


""



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

#通用函数完成，可新建文件保存，以后以导入自定义库方式引用函数"
