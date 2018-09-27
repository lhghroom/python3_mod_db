#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pypyodbc
import pymysql
import pymssql
import sqlite3
import os.path

import _mty


mdbErrString='' #供其它模块使用的全局变量了，实时保存了各函数执行时的错误信息

def msgbox(info,titletext='孤荷凌寒的DB模块对话框QQ578652607',style=0,isShowErrMsg=False):
    return _mty.msgboxGhlh(info,titletext,style,isShowErrMsg)

#连接网络数据库,目前支持mssql,mysql
def conNetdbGhlh(serveraddress,usr,pw,dbname,dbtype='mssql',isShowMsg=False):
    '''
    用于连接网络数据库，目前支持连接mssql,mysql两种网络关系型数据库，
    dbtype可选形参默认值是操作mssql，如果要连接Mysql则通过此可选形参指定：为mysql
    ,此函数返回一个connect数据库连接对象
    '''
    global mdbErrString
    mdbErrString=''
    try:
        if dbtype=='mssql':
            con=pymssql.connect(serveraddress,usr,pw,dbname,charset='utf8')
            return con
        elif dbtype=='mysql':
            con=pymysql.connect(serveraddress,usr,pw,dbname)
            return con
        else:
            return None

    except Exception as e:
        mdbErrString='连接网络数据库【' + serveraddress + '】【' + dbname + '】时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建,QQ578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return None
    else:
        pass
    finally:
        pass

#连接本地数据库文件，目前支持db,mdb,accdb,s3db
def conLocaldbGhlh(dbfilepath,strPass='',isShowMsg=False):
    '''
    连接本地数据库文件，目前支持mdb,accdb,以及sqlite数据库文件，识别方法是，如果有后缀mdb或accdb，则说明是access数据库文件，否则都认为是sqlite数据库文件。
    如果连接成功，将返回一个con数据库连接对象
    '''
    global mdbErrString
    mdbErrString=''
    try:
        strhznm=_mty.getFilehzGhlh(dbfilepath)
        if strhznm.find('mdb')>-1 or strhznm.find('accdb')>-1:
            #---连接access数据库----
            if strPass=='':
                strname='Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + dbfilepath
            else:
                strname='Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + dbfilepath + ';Pwd=' + strPass
            con=pypyodbc.connect(strname)
            return con
        else:
            #----连接sqlite数据库-----
            con=sqlite3.connect(dbfilepath)
            return con

    except Exception as e:
        mdbErrString='连接网络数据库文件【' + dbfilepath + '】时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return None
    else:
        pass
    finally:
        pass

#删除数据库中的表
def delTableGhlh(con,strtablenm,isShowMsg=False):
    '''
    此方法将删除指定conn中的table，不管table中是否有数据，因此 操作要谨慎
    ,成功返回True 失败返回False
    '''
    global mdbErrString
    mdbErrString=''

    try:
        if strtablenm is not None and strtablenm != '':
            sql = 'DROP TABLE IF EXISTS ' + strtablenm
            cur=con.curser
            cur.execute(sql)
            con.commit()
            cur.close()
            if isShowMsg==True:
                msgbox('删除数据库表[{}]成功!'.format(strtablenm))
            return True
        else:
            if isShowMsg==True:
                msgbox('the [{}] is empty or equal None!'.format(sql))
            return False
    except Exception as e:
        mdbErrString='删除数据库中表【' + strtablenm + '】时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return False
    else:
        pass
    finally:
        pass

#创建一个新表
def newTableGhlh(con,strTableNm,lstnm,lsttype,lstlength,strprimarykey,isAutoSetIDfieldAutoNumber=True,strSetFieldAutoNumberName='id',isDelExiTsTable = False, isShowMsg= False):
    '''
    传递有关表的每个字段的三大属性的分别的三个列表，
    并可以指定此表的PRIMARY key 字段，
    及指定是否自动识别ID字段为PRIMARY key 字段，
    如果要创建的表名是否存在，约定是否删除旧表
    如果设置为不删除旧表，则放弃新建表；
    '''
    global mdbErrString
    mdbErrString=''
    try:
        if strTableNm == "" or strTableNm.lower() == "select" or strTableNm.lower() == "from" or strTableNm.lower() == "where" or strTableNm.lower() == "order" or strTableNm.lower() == "insert" or strTableNm.lower() == "delete" or strTableNm.lower() == "in" or strTableNm.lower() == "with" or strTableNm.find("[") >-1 or strTableNm.find("]") >-1 :
            mdbErrString = "要创建的数据表名为空或为不合法的保留关键字，请重新确认数据表名。"
            if isShowMsg == True:
                msgbox(mdbErrString)
            return None

        if len(lstnm) != len(lsttype) or len(lsttype) != len(lstlength):
            mdbErrString = "在新建一个数据表时，接收到的三个关于表中字段属性的列表参数中元素总数不相同，无法执行。"
            if isShowMsg == True:
                msgbox(mdbErrString)
            
            return None
        #现在先检查表是否存在，如果存在，根据设置是删除旧表然后新建表呢，还是保留旧表而不新建表
        

    except Exception as e:
        mdbErrString='尝试创建表【' + strtablenm + '】时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return None
    else:
        pass
    finally:
        pass
            
#判断一个表在数据库中是否存在
def isTableExistGhlh(con,strtablenm,isShowMsg=False):
    '''
    判断一张表是否在数据库中存在
    ,需要传入con数据库连接对象
    '''
    global mdbErrString
    mdbErrString=''
    try:
        cura=con.cursor()
        return isTableExist2Ghlh(cura,strtablenm,isShowMsg)
        
    except Exception as e:
        mdbErrString='检查表【' + strtablenm + '】是否存在时出错（此错误一般说明表不存在）:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return False
    else:
        pass
    finally:
        try:
            cura.close
            #pass
        except:
            pass
        

#判断一个表在数据库中是否存在2
def isTableExist2Ghlh(cur,strtablenm,isShowMsg=False):
    '''
    判断一张表是否在数据库中存在
    ,需要传入数据库操作指针对象
    '''
    global mdbErrString
    mdbErrString=''
    try:
        strsql='SELECT * FROM ' + strtablenm + ';'
        cur.execute(strsql)
        return True
    except Exception as e:
        mdbErrString='检查表【' + strtablenm + '】是否存在时出错（此错误一般说明表不存在）:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return False
    else:
        pass
    finally:
        pass