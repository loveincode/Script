#!/usr/bin/env python
#encoding=utf-8
# -*- coding: UTF-8 -*-
#
import warnings
warnings.filterwarnings(action="ignore", message='the sets module is deprecated')
import sets
import json
import sys

# 需要更新为 C:\Python26\Lib\MySQLHelper.py 版本
from MySQLHelper import *
import traceback
import MySQLdb
import redis
import chardet
import time

if len(sys.argv) == 1:
    print "need more argvs"
    exit(1)

argv1 = sys.argv[1]

###################################################################################################

db0pool = redis.ConnectionPool(host='10.10.40.168',password='kedacom#123', port=63790, db=0)
db0Red = redis.StrictRedis(connection_pool=db0pool)

mysql = MySQLHelper("10.10.40.168","root","kdc",charset="utf8")
mysql.selectDb("vas")

mysql.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

quitCode={}
quitCode['noSuchMethod']     =1000
quitCode['loadPlatGroup']    =1001
quitCode['loadPau']          =1002
quitCode['deletePau']        =1003
quitCode['loadIau']          =1004
quitCode['deleteIau']        =1005
quitCode['loadPlatform']     =1006
quitCode['deletePlatform']   =1007

        


#同步普通分组
def loadPlatGroup():  
    try:
        sql = "SELECT * from PlatGroupTree order by CONVERT( GroupName USING GBK ) asc"
        results = mysql.queryAll(sql)
        
        platGroupKeyPrefix = str(db0Red.hincrby("SysCnf","PlatGroupSyncedCount",1)) + ":1:" 
        print 'platGroupKeyPrefix %s' %(platGroupKeyPrefix)
        
        db0pipe = db0Red.pipeline(transaction=True)
        Yindex=0
        resultsLen = len(results)
        for row in results:
            Yindex=Yindex+1
            
            row["GroupTypeID"] = 1 # PlatSyncCache --> PlatForm
            db0pipe.zadd(platGroupKeyPrefix + "ZgroupSort", Yindex, row["GroupID"]) #排序
            
            db0pipe.zadd(platGroupKeyPrefix + "ZgroupLeft", row["LeftScore"], row["GroupID"])
            db0pipe.zadd(platGroupKeyPrefix + "ZgroupRight", row["RightScore"], row["GroupID"])
            db0pipe.zadd(platGroupKeyPrefix + "ZgroupDepth", row["DepthScore"], row["GroupID"])
            db0pipe.hmset(platGroupKeyPrefix + "group:"+row["GroupID"],row)
            
            if 0 == Yindex%3000 or resultsLen == Yindex:
                db0pipe.execute()
                db0pipe = db0Red.pipeline(transaction=True)
        
        db0Red.lpush("PlatGroupKeyPrefix",platGroupKeyPrefix)

    except Exception,e:
       traceback.print_exc()
       quit(quitCode['loadPlatGroup'])
       
    
    
#增加pau
def loadPau(*IDArr):
    IDArr = IDArr[0] 
    
    sql = "SELECT PauID pauID, PauName pauName, PauIP pauIP, PauPort pauPort, PauCap pauCap,PauVersion pauVersion, PauPriority pauPriority FROM Pau "
    if len(IDArr) > 0:
        sql = sql + " WHERE PauId " + "in ( " + ",".join(["'%s'" % x for x in IDArr]) + ")"
    print sql
    
    sqlOrder = "SELECT PauID pauID FROM Pau ORDER BY CONVERT( PauName USING GBK ) ASC"
    print sqlOrder
    
    try:
        results = mysql.queryAll(sql)

        for row in results:
            for k in row.keys():
                if "None" == row[k]:
                    del row[k]
        
        #sqlOrder
        orderResults = mysql.queryAll(sqlOrder)

        rank=0
        for row in orderResults:
            rank=rank+1
            row["rank"] = rank
              
        lua =   """
                    local itemList = cjson.decode(ARGV[1])
                    for i,row in ipairs(itemList) do
                        
                        local param = {}
                        for k,v in pairs(row) do
                            param[#param+1] = k
                            param[#param+1] = v
                        end
                        redis.call("HMSET","pau:" .. row.pauID,unpack(param))
                        
                    end
                    
                    local orderList = cjson.decode(ARGV[2])
                    for i,row in ipairs(orderList) do
                        if redis.call("EXISTS","pau:" .. row.pauID) > 0 then
                            redis.call("ZADD","Zpau", row.rank , row.pauID )
                        end
                    end
	
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=[json.dumps(results), json.dumps(orderResults)])
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['loadPau'])
        
        
#删除pau
def deletePau(IDArr):
    try:
                      
        lua =   """
                    local pauIDs = {}
                    for i=1,#ARGV do
                        pauIDs[#pauIDs+1] = "pau:" .. ARGV[i]
                    end
                    
                    redis.call("DEL", unpack(pauIDs))
                    redis.call("ZREM", "Zpau", unpack(ARGV))
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=IDArr)
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['deletePau'])
        
        
        
#增加iau
def loadIau(*IDArr):
    IDArr = IDArr[0] 
    
    sql = "SELECT IauID iauID, IauName iauName, IauIP iauIP, IauPort iauPort, IauCap iauCap,IauVersion iauVersion, IauPriority iauPriority FROM Iau "
    if len(IDArr) > 0:
        sql = sql + " WHERE IauId " + "in ( " + ",".join(["'%s'" % x for x in IDArr]) + ")"
    print sql
    
    sqlOrder = "SELECT IauID iauID FROM Iau ORDER BY CONVERT( IauName USING GBK ) ASC"
    print sqlOrder
    
    try:
        results = mysql.queryAll(sql)

        for row in results:
            for k in row.keys():
                if "None" == row[k]:
                    del row[k]
        
        #sqlOrder
        orderResults = mysql.queryAll(sqlOrder)

        rank=0
        for row in orderResults:
            rank=rank+1
            row["rank"] = rank
              
        lua =   """
                    local itemList = cjson.decode(ARGV[1])
                    for i,row in ipairs(itemList) do
                        
                        local param = {}
                        for k,v in pairs(row) do
                            param[#param+1] = k
                            param[#param+1] = v
                        end
                        redis.call("HMSET","iau:" .. row.iauID,unpack(param))
                        
                    end
                    
                    local orderList = cjson.decode(ARGV[2])
                    for i,row in ipairs(orderList) do
                        if redis.call("EXISTS","iau:" .. row.iauID) > 0 then
                            redis.call("ZADD","Ziau", row.rank , row.iauID )
                        end
                    end
	
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=[json.dumps(results), json.dumps(orderResults)])
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['loadIau'])
        
        
#删除iau
def deleteIau(IDArr):
    try:
                      
        lua =   """
                    local iauIDs = {}
                    for i=1,#ARGV do
                        iauIDs[#iauIDs+1] = "iau:" .. ARGV[i]
                    end
                    
                    redis.call("DEL", unpack(iauIDs))
                    redis.call("ZREM", "Ziau", unpack(ARGV))
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=IDArr)
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['deleteIau'])
        
        
        
#增加platform
def loadPlatform(*IDArr):
    IDArr = IDArr[0] 
    
    sql = """
    SELECT PlatID platID, PlatName platName, PlatIP platIP, PlatPort platPort, PlatUserName platUserName,PlatUserPassword platUserPassword,
    b.PlatTypeID platTypeID,b.PlatTypeName platTypeName, PlatGBNO platGBNO, PlatKDMNO platKDMNO, IsTopPlat isTopPlat 
    FROM Platform a LEFT JOIN PlatformType b ON a.PlatTypeID = b.PlatTypeID
    
    """
    if len(IDArr) > 0:
        sql = sql + " WHERE PlatID " + "in ( " + ",".join(["'%s'" % x for x in IDArr]) + ")"
    print sql
    
    sqlOrder = "SELECT PlatID platID FROM Platform ORDER BY CONVERT( PlatName USING GBK ) ASC"
    print sqlOrder
    
    try:
        results = mysql.queryAll(sql)

        for row in results:
            for k in row.keys():
                if "None" == row[k]:
                    del row[k]
        
        #sqlOrder
        orderResults = mysql.queryAll(sqlOrder)

        rank=0
        for row in orderResults:
            rank=rank+1
            row["rank"] = rank
              
        lua =   """
                    local itemList = cjson.decode(ARGV[1])
                    for i,row in ipairs(itemList) do
                        
                        local param = {}
                        for k,v in pairs(row) do
                            param[#param+1] = k
                            param[#param+1] = v
                        end
                        redis.call("HMSET","platform:" .. row.platID,unpack(param))
                        
                    end
                    
                    local orderList = cjson.decode(ARGV[2])
                    for i,row in ipairs(orderList) do
                        if redis.call("EXISTS","platform:" .. row.platID) > 0 then
                            redis.call("ZADD","Zplatform", row.rank , row.platID )
                        end
                    end
	
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=[json.dumps(results), json.dumps(orderResults)])
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['loadPlatform'])
        
        
#删除platform
def deletePlatform(IDArr):
    try:
                      
        lua =   """
                    local platIDs = {}
                    for i=1,#ARGV do
                        platIDs[#platIDs+1] = "platform:" .. ARGV[i]
                    end
                    
                    redis.call("DEL", unpack(platIDs))
                    redis.call("ZREM", "Zplatform", unpack(ARGV))
                """
	
        script = db0Red.register_script(lua)
        script(keys=[],args=IDArr)
        
        
    except Exception,e:
        traceback.print_exc()
        quit(quitCode['deletePlatform'])
        
        
        
        
        
        
                
        
##############################################################

if argv1 == "loadPlatGroup":
    print 'begin loadPlatGroup'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    loadPlatGroup()
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print "loadPlatGroup end COST [%d]"   %(t2 - t1)

elif argv1 == "loadPau":
    print 'begin loadPau'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    loadPau(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("loadPau end")
    
elif argv1 == "deletePau":
    print 'begin deletePau'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    deletePau(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("deletePau end")
    

elif argv1 == "loadIau":
    print 'begin loadIau'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    loadIau(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("loadIau end")
    
elif argv1 == "deleteIau":
    print 'begin deleteIau'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    deleteIau(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("deleteIau end")
    
    
elif argv1 == "loadPlatform":
    print 'begin loadPlatform'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    loadPlatform(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("loadPlatform end")
    
elif argv1 == "deletePlatform":
    print 'begin deletePlatform'
    t1 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t1)) 
    
    deletePlatform(sys.argv[2:])
    
    t2 = time.time()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t2)) 
    print ("deletePlatform end")
    
    
else:
    quit(quitCode['noSuchMethod'])

mysql.close()
db0pool.disconnect()

