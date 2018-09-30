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
            sql = 'DROP TABLE ' + strtablenm
            cur=con.cursor()
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
def newTableGhlh(con,strTableNm,lstnm,lsttype,lstlength,dbtype='acc',lstNull=None,isDelExitsTable = False ,isAutoSetIDfieldAutoNumber=True,strSetFieldAutoNumberName='id',isShowMsg= False):
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
        cur=con.cursor()
        dbtype=dbtype.lower()
        if dbtype=='access':
            dbtype='acc'
    except:
        pass
    #--------------------------------------------------------
    try:
        if strTableNm == "" or strTableNm.lower() == "select" or strTableNm.lower() == "from" or strTableNm.lower() == "where" or strTableNm.lower() == "order" or strTableNm.lower() == "insert" or strTableNm.lower() == "delete" or strTableNm.lower() == "in" or strTableNm.lower() == "with" or strTableNm.find("[") >-1 or strTableNm.find("]") >-1 :
            mdbErrString = "要创建的数据表名为空或为不合法的保留关键字，请重新确认数据表名。" + '\n此函数由【孤荷凌寒】创建qq是578652607'
            if isShowMsg == True:
                msgbox(mdbErrString)
            return False

        if len(lstnm) != len(lsttype) or len(lsttype) != len(lstlength):
            mdbErrString = "在新建一个数据表时，接收到的四个关于表中字段属性的列表参数中元素总数不相同，无法执行。" + '\n此函数由【孤荷凌寒】创建qq号：578652607'
            if isShowMsg == True:
                msgbox(mdbErrString)
            
            return False

        #现在先检查表是否存在，如果存在，根据设置是删除旧表然后新建表呢，还是保留旧表而不新建表
        if isTableExistGhlh(con,strTableNm,isShowMsg)==True:
            #--如果旧表存在，就看是否要删除旧表---
            if isDelExitsTable==True:
                if delTableGhlh(con,strTableNm,isShowMsg)==False:
                    #--旧表存在，但是却删除失败的情况----
                    mdbErrString = "在新建一个数据表时，因为同名的旧表已经存在了，但尝试删除旧表失败，所以无法新增一个表。" + '\n此函数由【孤荷凌寒】创建qq：578652607'
                    if isShowMsg == True:
                        msgbox(mdbErrString)
                    return False
                else:
                    #成功删除了旧表，那么就添加新表，直接顺序到后面执行代码即可。
                    pass

            else:
                #如果旧表存在，但又指定不删除旧表，那么只好结束 本函数 过程了
                mdbErrString = "在新建一个数据表时，因为同名的旧表已经存在了，而又指定不能删除旧表，所以无法新增一个表。" + '\n此函数由【孤荷凌寒】创建qq是578652607'
                if isShowMsg == True:
                    msgbox(mdbErrString)
                return False

        #现在准备开始添加新的表-----
        intC=len(lstnm)
        rals=range(intC)
        strR=""
        strRls=""
        strNm=""
        strLs=""
        intL=0
        strL=""
        strN=""
        for i in rals:
            strNm=lstnm[i]
            strLs =lsttype[i]
            strLs = getStandardFieldTypeGhlh(strLs,dbtype,isShowMsg)
            strLs=' ' + strLs
            #-----------------------
            intL=lstlength[i]
            if intL<=0:
                strL=''
            else:
                strL="(" + str(intL) + ")"
            #----------------
            strN=""
            if lstNull != None:
                try:
                    strN=lstNull[i]
                except:
                    pass
                #---------------
                if strN=="" or strN==None:
                    strN=""
                else:
                    strN=" " + strN
            #----------
            if strLs.find('NULL')>=0:
                #-----如果已经在得到类别时，已经在字符串中出现了null关键字，此处就不要再处理了
                strN=""
            #---------------
            if dbtype!='mysql':
                #上一条件式是因为，Mysql不允许在sql语句中出现 []括号
                strNm='[' + strNm + ']'
            strRls=strNm + strLs + strL + strN # 此时已经构建了类似于 【name varchar(20)】  这样的内容了
            #检查是否主键--
            if isAutoSetIDfieldAutoNumber==True:
                #如果强制将字段名称为“id”的字段作为主键，则
                if strNm.lower()==strSetFieldAutoNumberName.lower():
                    if strR.find("PRIMARY KEY")<0:
                        #上一条件式是为了避免有多个primary key
                        if strRls.find("PRIMARY KEY")<0:
                            #上一条件式是为了防止在取得可用字段类型时已添加过Primary key 了
                            strRls=strRls+" PRIMARY KEY"
            
            #现在拼合 strR
            if strR=="":
                strR=strRls
            else:
                strR=strR + "," + strRls

        #开始生成sql语句
        strSql='CREATE TABLE ' + strTableNm + '(' + strR + ');'
        #运行--
        cur.execute(strSql)
        con.commit() #提交所作的修改
        #如果没有出错，就返回成功
        return True

            
            




    except Exception as e:
        mdbErrString='尝试创建表【' + strTableNm + '】时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        return False
    else:
        pass
    finally:
        try:
            cur.close()
        except:
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

#将各种复杂的对数据库类型的描述，如3,8等数值表示的字段类型与，windows系统中的system.string,之类的描述，统一修改为数据库能够在定义字段类型时直接使用的描述字符串
def getStandardFieldTypeGhlh(strin,dbtype='acc',isShowMsg=False):
    '''
    将各种复杂的对数据库类型的描述，如3,8等数值表示的字段类型与，windows系统中的system.string,之类的描述，统一修改为数据库能够在定义字段类型时直接使用的描述字符串
    '''
    global mdbErrString
    mdbErrString=''
    strI=""
    try:
        strI=str(strin)
        strI.lower()
        strI=strI.replace('system.','') #windows系统中，以及其它一些语言中对数据类型的描述的字符串中，可以包含有system.前缀
        strI=strI.replace('.','') #去掉多余的点
        dbtype=dbtype.lower()
        if dbtype=='access':
            dbtype='acc'
    except:
        pass
    #--------------------------------------------------------
    try:
        if strI=='':
            mdbErrString = "因为传入的要识别的数据库的字段类型为空，因此无法识别，只能识别成【文本类型】【text】。" + '\n此函数由【孤荷凌寒】创建qq：578652607'
            if isShowMsg == True:
                msgbox(mdbErrString)
            if dbtype!='acc' and dbtype!='mysql':
                return 'ntext'
            else:
                return 'text'
        #---正式识别开始---------------------
        if strI in ("int32", "3", "int","int16", "integer", "long","smallint","tinyint","mediumint"):
            if dbtype=='acc':
                return 'long'
            else:
                return "int"  #多数数据库在这种情况下要额外指定长度
        #----------------------
        if strI=='bigint':
            if dbtype=='acc' or dbtype=='sqlite':
                return 'int'
            else:
                return 'bigint'
        #-----------------
        elif strI in ("memo","longtext","mediumtext"):
            if dbtype=='acc':
                return "memo"
            elif dbtype=='mysql':
                return "longtext"
            else:
                return 'ntext'
        #------------------
        elif strI in ("str","string","8","varchar","char","text","nvarchar","tinytext"):
            if dbtype=='mysql' or dbtype=='acc':
                return "varchar"  #在这种情况下都需要指定长度
            else:
                return "nvarchar"  #在这种情况下都需要指定长度
        #------------------
        elif strI in ("datetime","7"):
            if dbtype=='sqlite':
                return "date"
            else:
                return "datetime"
        #----------------
        elif strI=="date":
            if dbtype!='acc':
                return "date"
            else:
                return "datetime"                
        #-----------------
        elif strI=="time":
            if dbtype!='acc':
                return "time"
            else:
                return "datetime"                

        #-----------------
        elif strI in ("single", "4", "real"):
            return "real"

        #----------------
        elif strI in ("double", "5", "float"):
            return "float"
        #----------------
        elif strI in ("boolean", "11", "bit","bool"):
            if dbtype=='mssql' or dbtype=='acc':
                return "bit"
            else:
                return 'boolean'
        #-----------------
        elif strI in ("byte[]", "8209", "image", "binary", "ole"):
            #---image为微软专用的OLE，"Binary" 为 二进制，在sqlite中使用blob,表示二进制大数据
            if dbtype=='acc' or dbtype=='mssql':
                return "Image"
            elif dbtype=='sqlite':
                return 'blob'
            else:
                return 'binary'
        #-------这是真正的全精度数据
        elif strI in ("decimal", "14", "money","numeric"):
            if dbtype=='sqlite':
                return 'numeric'
            elif dbtype=='acc':
                return 'money'
            else:
                return 'decimal'

        #--------------
        elif strI=="timestamp":
            if dbtype=='acc':
                return 'double'
            else:
                return 'timestamp'
            
        #------自动编号------       
        elif strI in ("auto", "autocount", "autonumber", "autono", "autoincrement","auto_increment"):
            if dbtype=='mysql':
                return 'int NOT NULL auto_increment'
            elif dbtype=='acc':
                return 'counter NOT NULL PRIMARY KEY'
            elif dbtype=='mssql':
                return 'int identity(1,1)'
            else:
                #--sqlite-----------------
                return "integer PRIMARY KEY AUTOINCREMENT NOT NULL"
        
        #--------
        else:
            #其余情况，全部识别为 text
            if dbtype!='acc' and dbtype!='mysql':
                return 'ntext'
            else:
                return 'text'


    except Exception as e:
        mdbErrString='尝试将各种不同的对数据库字段类型的描述转换为标准字段类型描述时出错:' + str(e) + '\n此函数由【孤荷凌寒】创建qq号是578652607'
        if isShowMsg==True:
           msgbox(mdbErrString)
        #------------------------------------------
        if dbtype!='acc' and dbtype!='mysql':
            return 'ntext'
        else:
            return 'text'
    else:
        pass
    finally:
        pass
        


