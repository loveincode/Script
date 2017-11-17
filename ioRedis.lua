local rdebug =     redis.LOG_DEBUG
local rverb =     redis.LOG_VERBOSE
local rntf =    redis.LOG_NOTICE
local rwarn =     redis.LOG_WARNING

local rlog = function(m)
  redis.log(rntf,m)
end
-- 待完成， admin的处理，  搜索，分页查询
local function printTab(tab)
  for i,v in pairs(tab) do
    if type(v) == "table" then
      rlog("table " .. i .. "{")
      printTab(v)
      rlog("}")
    else
      rlog(i .. " -- " .. tostring(v))
    end
  end
end


-- rlog("KEYS:".. #KEYS );
for i=1,#KEYS
do
  -- rlog(KEYS[i])
end

-- rlog("ARGV:" .. #ARGV)
local argvsg = ''
for i=1,#ARGV
do
  -- rlog(type(ARGV[i]))
  -- rlog(ARGV[i])
end

--  function
local debug_flag = true
local function debug(msg,t,ZorS)
  if debug_flag then
    ZorS = ZorS or "Z"
    -- rlog("DEBUG]] " ..msg)
    if type(t) == "table" then
      t = t or {}
      printTab(t)
    elseif  type(t) == "string" then
      if ZorS == "Z" then
        t = redis.call("ZRANGE",t,0,-1)
      elseif ZorS == "S" then
        t = redis.call("SMEMBERS",t)
      end
      printTab(t)
    end
    -- rlog("\n")
  end
end

local function unionRedisSet(ta,dst,step)
  redis.call("DEL",dst )
  
  local step = step or 1000

  for i=1,#ta,step do
    local packTail = math.min(i+step-1,#ta)
    local packSzie = packTail - i + 1
    redis.call("SUNIONSTORE",dst, dst,unpack(ta,i,packTail) )
  end
end

local function unionRedisZSet(ta,dst,step)
  local step = step or 1000

  redis.call("DEL",dst)

  for i=1,#ta,step do
    local packTail = math.min(i+step-1,#ta)
    local packSzie = packTail - i + 1
    redis.call("ZUNIONSTORE",dst,packSzie+1,dst,unpack(ta,i,packTail) )
  end
end


local function deleteMegaFromSet(ta,dst,step)
  local step = step or 1000
  
  local tp = redis.call("TYPE",dst)
  
  -- 不强制删除
  -- redis.call("DEL",dst)

  for i=1,#ta,step do
    local packTail = math.min(i+step-1,#ta)
    local packSzie = packTail - i + 1
    if tp.ok == "zset" then
      redis.call("ZREM",dst,unpack(ta,i,packTail) )
    elseif tp.ok == "set" then
      redis.call("SREM",dst,unpack(ta,i,packTail) )
    end
  end
end


local function saddTable(ta,dst,step)
  local step = step or 1000

  for i=1,#ta,step do
    local packTail = math.min(i+step-1,#ta)
    local packSzie = packTail - i + 1
    redis.call("SADD",dst,unpack(ta,i,packTail) )
  end
end

-- saddTable({1,"a",2,"b"},"ta")

local function convertToTable(t)
  local tmp = {}

  for c = 1, #t, 2 do
    tmp[t[c]] = t[c + 1]
  end
  return tmp
end

local function tableIntersection(...)
	local t = {}
	
	for i = 1,arg.n do
	   	for i,v in pairs(arg[i]) do
			t[v] = (t[v] or 0) + 1
	    end
	end

	local r = {}
	for i,v in pairs(t) do
		if arg.n == v then
			r[#r+1] = i
		end
	end
	
	return r
end

local function getPlatGroupKeyPrefix()
    local p = redis.call("LRANGE", "PlatGroupKeyPrefix", 0 ,0)
	return p[1]
end


local function getChildGroup(groupPrefix, parentGroupID, lv)

      --     db0pipe.zadd(platGroupKeyPrefix + "ZgroupSort", Yindex, row["GroupID"]) #排序
      --     
      --     db0pipe.zadd(platGroupKeyPrefix + "ZgroupLeft", row["LeftScore"], row["GroupID"])
      --     db0pipe.zadd(platGroupKeyPrefix + "ZgroupRight", row["RightScore"], row["GroupID"])
      --     db0pipe.zadd(platGroupKeyPrefix + "ZgroupDepth", row["DepthScore"], row["GroupID"])
      --
  groupPrefix = groupPrefix or ""

  local childGroupID = {}
  
  local parentGroup = redis.call("HGETALL",groupPrefix .. "group:" .. parentGroupID)
  if next(parentGroup) ~= nil then

    parentGroup = convertToTable(parentGroup)

    local leftSet = {}
    local rightSet = {}
    local depthSet = {}

	leftSet = redis.call("ZRANGEBYSCORE", groupPrefix .. "ZgroupLeft", parentGroup["LeftScore"] ,'+inf')
	
	rightSet = redis.call("ZRANGEBYSCORE", groupPrefix .. "ZgroupRight",'-inf', parentGroup["RightScore"])
	
    if(-1 == lv + 0) then
        depthSet = redis.call("ZRANGEBYSCORE", groupPrefix .. "ZgroupDepth",parentGroup["DepthScore"] + 1,'+inf')
    else
        depthSet = redis.call("ZRANGEBYSCORE", groupPrefix .. "ZgroupDepth",parentGroup["DepthScore"] + 1,parentGroup["DepthScore"] + lv)
    end
    
	local innerSet = tableIntersection(leftSet, rightSet, depthSet)
	
	
	local tempKeyPostfix = redis.call("CONFIG","GET","PORT")[2]
    local tempKeySort = "T-getchildgroup-sort" .. tempKeyPostfix
	redis.call("DEL", tempKeySort)
    saddTable(innerSet,tempKeySort)
	
    redis.call("ZINTERSTORE",tempKeySort, 2 ,tempKeySort,groupPrefix .. "ZgroupSort",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
	
    childGroupID = redis.call("ZRANGE",tempKeySort,0,-1)
    
  end

  return childGroupID
end


local function getPlatChildGroup(parentGroupID, lv)
    lv = lv or 1
    local pgkp  = getPlatGroupKeyPrefix()
    if not pgkp then
	  return {0,{}}
	end

    local ids = getChildGroup(pgkp, parentGroupID, lv)
  return {#ids, ids}
end 

local function getPauByIDArray(...)
    local IDs = {}
    for i = 1,arg.n do
       IDs[#IDs+1] = arg[i]
    end

    local result = {}

    for i,id  in pairs(IDs) do
        local obj = redis.call("HGETALL","pau:" .. id)
        if next(obj) ~= nil then
            obj = convertToTable(obj)
            
            obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
            
            result[#result+1] = obj
        else
            result[#result+1] = false
        end
    end

    return  {cjson.encode(result)}
end


local function searchPau(pageNum, pageSize, orderBy, orderScend, c_name , c_netAddress , c_OnlineStatus, c_version, c_cap )
 
    local total = redis.call("ZCARD", "Zpau")
    pageNum = tonumber(pageNum)
    pageSize = tonumber(pageSize)
    if pageNum < 0 then
        pageNum = 1
    end
    
    if pageSize < 0 then
        pageSize = 1
    end
 
 -- get all matched
    if 0 == pageNum*pageSize then
        pageNum = 1
        pageSize = total
    end
 
    local resultIDs = {}
    
    local q_fields = {}
    
    local flag_name = 0
    if (c_name and string.len(c_name) > 0) then 
      flag_name = 1 
      c_name = string.lower(c_name)
      q_fields.Name = #q_fields+1
      q_fields[#q_fields+1] = "pauName"
    end
    
    local flag_netAddress = 0
    if (c_netAddress and string.len(c_netAddress) > 0) then 
      flag_netAddress = 1 
      q_fields.NetAddress = #q_fields+1
      q_fields[#q_fields+1] = "pauID"
    end 
    
    local flag_OnlineStatus = 0
    if (c_OnlineStatus and string.len(c_OnlineStatus) > 0) then 
      flag_OnlineStatus = 1 
      q_fields.onlineStatus = #q_fields+1
      q_fields[#q_fields+1] = "onlineStatus"
    end  
    
    local flag_version = 0
    if (c_version and string.len(c_version) > 0) then
      flag_version = 1 
      c_version = string.lower(c_version)
      q_fields.Version = #q_fields+1
      q_fields[#q_fields+1] = "pauVersion"
    end
    
    local flag_cap = 0
    if (c_cap and string.len(c_cap) > 0) then
      flag_cap = 1 
      c_cap = string.lower(c_cap)
      q_fields.Capability = #q_fields+1
      q_fields[#q_fields+1] = "pauCap"
    end
    
    local flag_condtion_total = flag_name  + flag_netAddress  + flag_OnlineStatus  + flag_version + flag_cap
  
    local zrangeScent = "ZRANGE"
    
    -- orderBy if not pauName then use "Zpau-orderBy"
    
    if (orderScend and "DESC" == string.upper(orderScend)) then 
        zrangeScent = "ZREVRANGE"
    end 
  
    if flag_condtion_total > 0 then -- 需要过滤
        local IDs = redis.call(zrangeScent, "Zpau", 0 , -1)
        
        total = 0
        
        local flag_math = 0
        
        for i,v  in pairs(IDs) do
        
            local m_dev = redis.call("HMGET","pau:" .. v, unpack(q_fields))
        
            flag_math = 0
            
            local sf =  string.find
            
            if flag_name > 0 and sf(string.lower(m_dev[q_fields.Name]), c_name,1,true) then
            flag_math = flag_math + 1
            end
        
            if flag_netAddress > 0 and m_dev[q_fields.NetAddress] and sf(m_dev[q_fields.NetAddress], c_netAddress,1,true) then
            flag_math = flag_math + 1
            end
            
            if flag_OnlineStatus > 0 and ((not m_dev[q_fields.onlineStatus] and c_OnlineStatus == "0") or (m_dev[q_fields.onlineStatus] == c_OnlineStatus)) then
            flag_math = flag_math + 1
            end
        
            if flag_version > 0 and m_dev[q_fields.Version] and sf(string.lower(m_dev[q_fields.Version]), c_version,1,true) then
            flag_math = flag_math + 1
            end
        
            
            if flag_cap then
                repeat
                    if not m_dev[q_fields.Capability] then
                        break 
                    end
                
                    local temp_cap = string.lower(m_dev[q_fields.Capability])
                
                    local it = string.gmatch(c_cap,"%(%w+%)")
                    
                    if not it() then
                        break
                    else
                        it = string.gmatch(c_cap,"%(%w+%)")
                    end
                    
                    local missed_cap = false
                    for w in it do 
                        if  not sf(temp_cap,  string.lower(w) ,1,true)  then
                            missed_cap = true
                            break
                        end
                    end
                    
                    if missed_cap then
                        break
                    end
                    
                    flag_math = flag_math + 1
                until true
            
            end
            
            if flag_math == flag_condtion_total then
                total = total + 1
                if total > (pageNum-1)*pageSize and total <= pageNum*pageSize then
                    resultIDs[#resultIDs+1] = v
                end
            end
        end
    else
        resultIDs = redis.call(zrangeScent, "Zpau", (pageNum-1)*pageSize , pageNum*pageSize -1 )
    end
    
    local result = {}
    
    for i,v in pairs(resultIDs) do
        local obj = redis.call("HGETALL","pau:" .. resultIDs[i])
        obj = convertToTable(obj)
        obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
        result[#result+1] = obj
    end
    
    local result_json = "[]"
    if #result > 0 then
        result_json = cjson.encode(result)
    end

    local rsp = {
        total = total,
        pageNum = pageNum,
        pageSize = pageSize,
        rspSize = #result,
        orderBy = orderBy,
        orderScend = string.upper(orderScend),
        pauList = result
    }
    
    -- local rspp = {total,#result,result_json}
    
    return  {cjson.encode(rsp)}

end


local function pauLogin(heartbeat,pauID, pauIP)

    local pauKey = "pau:" .. pauID
    if redis.call("EXISTS", pauKey) > 0 then
        local mdata = redis.call("HMGET", pauKey, "pauIP", "onlineStatus" )
        if not (mdata[1] and pauIP == mdata[1] ) then
            return {false,"IPNotMatch"}
        elseif mdata[2] and "1" == mdata[2] then
            return {false,"AlreadyOnline"}
        else
            redis.call("HMSET", pauKey, "heartbeat", heartbeat, "onlineStatus", 1 )
            
            local obj = redis.call("HGETALL", pauKey)
            
            if next(obj) ~= nil then
                obj = convertToTable(obj)
                obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
                return {true,cjson.encode(obj)}
            else
                return {false, "Unknown"}
            end
        end
    else
        return {false,"NotExists"}
    end

end


local function pauLogout(pauID, pauIP)
    local pauKey = "pau:" .. pauID
    redis.call("HDEL", pauKey, "onlineStatus", "heartbeat" )
    
    return {true,"OK"}

end

local function pauHeartBeat(heartbeat, pauID, pauIP)
    local pauKey = "pau:" .. pauID
    if redis.call("EXISTS", pauKey) > 0 then
        local mdata = redis.call("HMGET", pauKey, "pauIP", "onlineStatus" )
        if not (mdata[1] and pauIP == mdata[1] ) then
            return {false,"IPNotMatch"}
        elseif not (mdata[2] and "1" == mdata[2] ) then
            return {false,"NotOnline"}
        else
            redis.call("HSET", pauKey, "heartbeat", heartbeat )
            return {true,"OK"}
        end
    else
        return {false,"NotExists"}
    end

end


local function getIauByIDArray(...)
    local IDs = {}
    for i = 1,arg.n do
       IDs[#IDs+1] = arg[i]
    end

    local result = {}

    for i,id  in pairs(IDs) do
        local obj = redis.call("HGETALL","iau:" .. id)
        if next(obj) ~= nil then
            obj = convertToTable(obj)
            
            obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
            
            result[#result+1] = obj
        else
            result[#result+1] = false
        end
    end

    return  {cjson.encode(result)}
end


local function searchIau(pageNum, pageSize, orderBy, orderScend, c_name , c_netAddress , c_OnlineStatus, c_version, c_cap )
 
    local total = redis.call("ZCARD", "Ziau")
    pageNum = tonumber(pageNum)
    pageSize = tonumber(pageSize)
    if pageNum < 0 then
        pageNum = 1
    end
    
    if pageSize < 0 then
        pageSize = 1
    end
 
 -- get all matched
    if 0 == pageNum*pageSize then
        pageNum = 1
        pageSize = total
    end
 
    local resultIDs = {}
    
    local q_fields = {}
    
    local flag_name = 0
    if (c_name and string.len(c_name) > 0) then 
      flag_name = 1 
      c_name = string.lower(c_name)
      q_fields.Name = #q_fields+1
      q_fields[#q_fields+1] = "iauName"
    end
    
    local flag_netAddress = 0
    if (c_netAddress and string.len(c_netAddress) > 0) then 
      flag_netAddress = 1 
      q_fields.NetAddress = #q_fields+1
      q_fields[#q_fields+1] = "iauID"
    end 
    
    local flag_OnlineStatus = 0
    if (c_OnlineStatus and string.len(c_OnlineStatus) > 0) then 
      flag_OnlineStatus = 1 
      q_fields.onlineStatus = #q_fields+1
      q_fields[#q_fields+1] = "onlineStatus"
    end  
    
    local flag_version = 0
    if (c_version and string.len(c_version) > 0) then
      flag_version = 1 
      c_version = string.lower(c_version)
      q_fields.Version = #q_fields+1
      q_fields[#q_fields+1] = "iauVersion"
    end
    
    local flag_cap = 0
    if (c_cap and string.len(c_cap) > 0) then
      flag_cap = 1 
      c_cap = string.lower(c_cap)
      q_fields.Capability = #q_fields+1
      q_fields[#q_fields+1] = "iauCap"
    end
    
    local flag_condtion_total = flag_name  + flag_netAddress  + flag_OnlineStatus  + flag_version + flag_cap
  
    local zrangeScent = "ZRANGE"
    
    -- orderBy if not iauName then use "Ziau-orderBy"
    
    if (orderScend and "DESC" == string.upper(orderScend)) then 
        zrangeScent = "ZREVRANGE"
    end 
  
    if flag_condtion_total > 0 then -- 需要过滤
        local IDs = redis.call(zrangeScent, "Ziau", 0 , -1)
        
        total = 0
        
        local flag_math = 0
        
        for i,v  in pairs(IDs) do
        
            local m_dev = redis.call("HMGET","iau:" .. v, unpack(q_fields))
        
            flag_math = 0
            
            local sf =  string.find
            
            if flag_name > 0 and sf(string.lower(m_dev[q_fields.Name]), c_name,1,true) then
            flag_math = flag_math + 1
            end
        
            if flag_netAddress > 0 and m_dev[q_fields.NetAddress] and sf(m_dev[q_fields.NetAddress], c_netAddress,1,true) then
            flag_math = flag_math + 1
            end
            
            if flag_OnlineStatus > 0 and ((not m_dev[q_fields.onlineStatus] and c_OnlineStatus == "0") or (m_dev[q_fields.onlineStatus] == c_OnlineStatus)) then
            flag_math = flag_math + 1
            end
        
            if flag_version > 0 and m_dev[q_fields.Version] and sf(string.lower(m_dev[q_fields.Version]), c_version,1,true) then
            flag_math = flag_math + 1
            end
        
            
            if flag_cap then
                repeat
                    if not m_dev[q_fields.Capability] then
                        break 
                    end
                
                    local temp_cap = string.lower(m_dev[q_fields.Capability])
                
                    local it = string.gmatch(c_cap,"%(%w+%)")
                    
                    if not it() then
                        break
                    else
                        it = string.gmatch(c_cap,"%(%w+%)")
                    end
                    
                    local missed_cap = false
                    for w in it do 
                        if  not sf(temp_cap,  string.lower(w) ,1,true)  then
                            missed_cap = true
                            break
                        end
                    end
                    
                    if missed_cap then
                        break
                    end
                    
                    flag_math = flag_math + 1
                until true
            
            end
            
            if flag_math == flag_condtion_total then
                total = total + 1
                if total > (pageNum-1)*pageSize and total <= pageNum*pageSize then
                    resultIDs[#resultIDs+1] = v
                end
            end
        end
    else
        resultIDs = redis.call(zrangeScent, "Ziau", (pageNum-1)*pageSize , pageNum*pageSize -1 )
    end
    
    local result = {}
    
    for i,v in pairs(resultIDs) do
        local obj = redis.call("HGETALL","iau:" .. resultIDs[i])
        obj = convertToTable(obj)
        obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
        result[#result+1] = obj
    end
    
    local result_json = "[]"
    if #result > 0 then
        result_json = cjson.encode(result)
    end

    local rsp = {
        total = total,
        pageNum = pageNum,
        pageSize = pageSize,
        rspSize = #result,
        orderBy = orderBy,
        orderScend = string.upper(orderScend),
        iauList = result
    }
    
    -- local rspp = {total,#result,result_json}
    
    return  {cjson.encode(rsp)}

end


local function iauLogin(heartbeat,iauID, iauIP)

    local iauKey = "iau:" .. iauID
    if redis.call("EXISTS", iauKey) > 0 then
        local mdata = redis.call("HMGET", iauKey, "iauIP", "onlineStatus" )
        if not (mdata[1] and iauIP == mdata[1] ) then
            return {false,"IPNotMatch"}
        elseif mdata[2] and "1" == mdata[2] then
            return {false,"AlreadyOnline"}
        else
            redis.call("HMSET", iauKey, "heartbeat", heartbeat, "onlineStatus", 1 )
            
            local obj = redis.call("HGETALL", iauKey)
            
            if next(obj) ~= nil then
                obj = convertToTable(obj)
                obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
                return {true,cjson.encode(obj)}
            else
                return {false, "Unknown"}
            end
        end
    else
        return {false,"NotExists"}
    end

end


local function iauLogout(iauID, iauIP)
    local iauKey = "iau:" .. iauID
    redis.call("HDEL", iauKey, "onlineStatus", "heartbeat" )
    
    return {true,"OK"}

end

local function iauHeartBeat(heartbeat, iauID, iauIP)
    local iauKey = "iau:" .. iauID
    if redis.call("EXISTS", iauKey) > 0 then
        local mdata = redis.call("HMGET", iauKey, "iauIP", "onlineStatus" )
        if not (mdata[1] and iauIP == mdata[1] ) then
            return {false,"IPNotMatch"}
        elseif not (mdata[2] and "1" == mdata[2] ) then
            return {false,"NotOnline"}
        else
            redis.call("HSET", iauKey, "heartbeat", heartbeat )
            return {true,"OK"}
        end
    else
        return {false,"NotExists"}
    end

end




local function getPlatGroupByIDArray(...)

    local pgkp  = getPlatGroupKeyPrefix()
    if not pgkp then
	  return  {"[]"}
	end

    local IDs = {}
    for i = 1,arg.n do
       IDs[#IDs+1] = arg[i]
    end

    local result = {}

    for i,id  in pairs(IDs) do
        local obj = redis.call("HGETALL",pgkp .. "group:" .. id)
        if next(obj) ~= nil then
            obj = convertToTable(obj)
            result[#result+1] = obj
        else
            result[#result+1] = false
        end
    end

    return  {cjson.encode(result)}
end


local function getPlatformByIDArray(...)
    local IDs = {}
    for i = 1,arg.n do
       IDs[#IDs+1] = arg[i]
    end

    local result = {}

    for i,id  in pairs(IDs) do
        local obj = redis.call("HGETALL","platform:" .. id)
        if next(obj) ~= nil then
            obj = convertToTable(obj)
            
            obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
            
            result[#result+1] = obj
        else
            result[#result+1] = false
        end
    end

    return  {cjson.encode(result)}
end

local function searchPlatform(pageNum, pageSize, orderBy, orderScend, c_name , c_netAddress , c_OnlineStatus, c_version, c_cap )
    local total = redis.call("ZCARD", "Zplatform")
    pageNum = tonumber(pageNum)
    pageSize = tonumber(pageSize)
    if pageNum < 0 then
        pageNum = 1
    end
    
    if pageSize < 0 then
        pageSize = 1
    end
 
 -- get all matched
    if 0 == pageNum*pageSize then
        pageNum = 1
        pageSize = total
    end
 
    local resultIDs = {}
    
    local q_fields = {}
    
    local flag_name = 0
    if (c_name and string.len(c_name) > 0) then 
      flag_name = 1 
      c_name = string.lower(c_name)
      q_fields.Name = #q_fields+1
      q_fields[#q_fields+1] = "platName"
    end
    
    local flag_netAddress = 0
    if (c_netAddress and string.len(c_netAddress) > 0) then 
      flag_netAddress = 1 
      q_fields.NetAddress = #q_fields+1
      q_fields[#q_fields+1] = "platID"
    end 
    
    local flag_OnlineStatus = 0
    if (c_OnlineStatus and string.len(c_OnlineStatus) > 0) then 
      flag_OnlineStatus = 1 
      q_fields.onlineStatus = #q_fields+1
      q_fields[#q_fields+1] = "onlineStatus"
    end  
    
    local flag_version = 0
    if (c_version and string.len(c_version) > 0) then
      flag_version = 1 
      c_version = string.lower(c_version)
      q_fields.Version = #q_fields+1
      q_fields[#q_fields+1] = "platVersion"
    end
    
    local flag_cap = 0
    if (c_cap and string.len(c_cap) > 0) then
      flag_cap = 1 
      c_cap = string.lower(c_cap)
      q_fields.Capability = #q_fields+1
      q_fields[#q_fields+1] = "platCap"
    end
    
    local flag_condtion_total = flag_name  + flag_netAddress  + flag_OnlineStatus  + flag_version + flag_cap
  
    local zrangeScent = "ZRANGE"
    
    -- orderBy if not platName then use "Zplatform-orderBy"
    
    if (orderScend and "DESC" == string.upper(orderScend)) then 
        zrangeScent = "ZREVRANGE"
    end 
  
    if flag_condtion_total > 0 then -- 需要过滤
        local IDs = redis.call(zrangeScent, "Zplatform", 0 , -1)
        
        total = 0
        
        local flag_math = 0
        
        for i,v  in pairs(IDs) do
        
            local m_dev = redis.call("HMGET","platform:" .. v, unpack(q_fields))
        
            flag_math = 0
            
            local sf =  string.find
            
            if flag_name > 0 and sf(string.lower(m_dev[q_fields.Name]), c_name,1,true) then
            flag_math = flag_math + 1
            end
        
            if flag_netAddress > 0 and m_dev[q_fields.NetAddress] and sf(m_dev[q_fields.NetAddress], c_netAddress,1,true) then
            flag_math = flag_math + 1
            end
            
            if flag_OnlineStatus > 0 and ((not m_dev[q_fields.onlineStatus] and c_OnlineStatus == "0") or (m_dev[q_fields.onlineStatus] == c_OnlineStatus)) then
            flag_math = flag_math + 1
            end
        
            if flag_version > 0 and m_dev[q_fields.Version] and sf(string.lower(m_dev[q_fields.Version]), c_version,1,true) then
            flag_math = flag_math + 1
            end
        
            
            if flag_cap then
                repeat
                    if not m_dev[q_fields.Capability] then
                        break 
                    end
                
                    local temp_cap = string.lower(m_dev[q_fields.Capability])
                
                    local it = string.gmatch(c_cap,"%(%w+%)")
                    
                    if not it() then
                        break
                    else
                        it = string.gmatch(c_cap,"%(%w+%)")
                    end
                    
                    local missed_cap = false
                    for w in it do 
                        if  not sf(temp_cap,  string.lower(w) ,1,true)  then
                            missed_cap = true
                            break
                        end
                    end
                    
                    if missed_cap then
                        break
                    end
                    
                    flag_math = flag_math + 1
                until true
            
            end
            
            if flag_math == flag_condtion_total then
                total = total + 1
                if total > (pageNum-1)*pageSize and total <= pageNum*pageSize then
                    resultIDs[#resultIDs+1] = v
                end
            end
        end
    else
        resultIDs = redis.call(zrangeScent, "Zplatform", (pageNum-1)*pageSize , pageNum*pageSize-1)
    end
    
    local result = {}
    
    for i,v in pairs(resultIDs) do
        local obj = redis.call("HGETALL","platform:" .. resultIDs[i])
        obj = convertToTable(obj)
        obj.status = ("1" == obj.onlineStatus and {"1"} or {"0"})[1]
        result[#result+1] = obj
    end
    
    local result_json = "[]"
    if #result > 0 then
        result_json = cjson.encode(result)
    end

    local rsp = {
        total = total,
        pageNum = pageNum,
        pageSize = pageSize,
        rspSize = #result,
        orderBy = orderBy,
        orderScend = string.upper(orderScend),
        platformList = result
    }
    
    -- local rspp = {total,#result,result_json}
    
    return  {cjson.encode(rsp)}
end

local switch = {
  getPlatChildGroup = getPlatChildGroup,
  getPlatGroupKeyPrefix = getPlatGroupKeyPrefix,
  getPlatGroupByIDArray = getPlatGroupByIDArray,
    
  getPauByIDArray = getPauByIDArray,
  searchPau = searchPau,
  pauLogin = pauLogin,
  pauLogout = pauLogout,
  pauHeartBeat = pauHeartBeat,
  
  getIauByIDArray = getIauByIDArray,
  searchIau = searchIau,
  iauLogin = iauLogin,
  iauLogout = iauLogout,
  iauHeartBeat = iauHeartBeat,
  
  searchPlatform = searchPlatform,
  getPlatformByIDArray = getPlatformByIDArray,
  
  niltail = nil
}


local cmd = switch[KEYS[1]]
if(cmd) then
  return cmd(unpack(KEYS,2))
else
  return "no such method"
end
    
