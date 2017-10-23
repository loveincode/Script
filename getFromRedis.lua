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


-- "getGroup" userid parentGroupID lv
-- "searchGroup" userid groupName
-- "getDevice" userid GroupID start end CriteriaName

-- 业务 function


local function getTempKey(userID)

	userID = userID or ""

	local pf = redis.call("CONFIG","GET","PORT")[2]
	
	local tempKeySet = {tempPrefix=pf}

	tempKeySet.key_user_dev = pf .. "Suserdevice:" .. userID

	-- user对应设备id集合，只包含role直接对应的device
	tempKeySet.key_user_drct_dev_from_role = pf .. "Suserdevice:roledevice:" .. userID

	-- user对应设备id集合，只包含role拥有的group中的device
	tempKeySet.key_user_dev_from_role_group = pf .. "Zuserdevice:rolegroup:" .. userID

	-- 完整路径分组
	tempKeySet.key_user_group_full_path = pf .. "Zusergroup:" .. userID
	tempKeySet.key_user_group_full_Lft = tempKeySet.key_user_group_full_path .. ":Lft"
	tempKeySet.key_user_group_full_Rgt = tempKeySet.key_user_group_full_path .. ":Rgt"
	tempKeySet.key_user_group_full_Depth = tempKeySet.key_user_group_full_path .. ":Depth"

	-- 角色对应的设备和分组组成的直接分组
	tempKeySet.key_user_group_no_path = pf .. "Zusergroup:roall:" .. userID
	tempKeySet.key_user_group_no_path_Lft = tempKeySet.key_user_group_no_path .. ":Lft"
	tempKeySet.key_user_group_no_path_Rgt = tempKeySet.key_user_group_no_path .. ":Rgt"
	tempKeySet.key_user_group_no_path_Depth = tempKeySet.key_user_group_no_path .. ":Depth"


	-- user对应分组id集合，只包含role直接对应的device所属分组
	tempKeySet.key_user_group_from_role_drct_dev = pf .. "Susergroup:roledevice:" .. userID

	-- user对应分组id集合，只包含role拥有的group
	tempKeySet.key_user_group_from_role_drct_group = pf .. "Zusergroup:rolegroup:" .. userID


	return tempKeySet


end



-- 清除用户缓存数据
local function cleanupUserTempData(userid)

	if not userid then

		userid = redis.call("SMEMBERS", "alluserid")

	end

	if "string" == type(userid) then
		userid = {userid}
	end
	
	local ta = {}
	
	for i,v in pairs (userid) do
		local tempKeySet = getTempKey(v)
		
		for i,v in pairs(tempKeySet) do
			if i ~= "tempPrefix" then
				ta[#ta+1] = v
			end
		end

	end

	local step = 1000
	for i=1,#ta,step do
		local packTail = math.min(i+step-1,#ta)
		local packSzie = packTail - i + 1
		redis.call("DEL", unpack(ta,i,packTail) )
	end

end



-- 获取角色
local function getRoleByUserID(userID)
  local role = redis.call("SMEMBERS","Suserrole:" .. userID)
  -- debug("user role",role)
  return role
end





-- 计算所有角色的直接设备的并集
local function genRoleDevice_UnionSet(role,dst)

    local l_ro_devIDs = {}
    for i,v in pairs(role) do
      l_ro_devIDs[#l_ro_devIDs+1] = "Sroledevice:" .. v
    end

    unionRedisSet(l_ro_devIDs,dst)

end

--获取角色直属分组
local function genRole_InlineGroup_UnionSet(role,dst)


    redis.call("DEL",dst)
    -- 角色对应分组的key集合
    local l_ro_grpIDs = {}

    for i,roleID in pairs(role) do
      l_ro_grpIDs[#l_ro_grpIDs+1] = "Srolegroup:".. roleID
    end

    -- 计算所有角色的直接分组的并集
    unionRedisSet(l_ro_grpIDs,dst)

    local temp_list = redis.call("SMEMBERS",dst )
    local needDel = {} -- 空分组不显示

    for i, roleID in pairs(temp_list) do
      if 0 == redis.call("SCARD","Sgroupdevice:" .. roleID) then
        needDel[#needDel+1] = roleID
      end
    end

    deleteMegaFromSet(needDel,dst)

end


    -- 获取所有角色直属分组的所有设备
local function genRoleDevice_InlineGroup_UnionSet(src,dst)

    local r_grp_devIDs_rogrp = {}

    for i,v in pairs(src) do
      r_grp_devIDs_rogrp[i] = "Sgroupdevice:"..v
    end

    unionRedisSet(r_grp_devIDs_rogrp,dst)

end

-- 直属设备对应分组的合集
local function genRole_DeviceLinkGroup_UnionSet(src,dst)

    redis.call("DEL",dst)
    -- 获取所有角色直属设备对应的分组
    local l_usr_devIDs_rodev = redis.call("SMEMBERS",src)

    local temp_userGroupIDSet = {}


    for i,v  in pairs(l_usr_devIDs_rodev) do
      local deviceGroup = redis.call("HGET","device:" .. v , "group")
      if(deviceGroup and temp_userGroupIDSet[deviceGroup] ~= 1) then
        temp_userGroupIDSet[deviceGroup] = 1
      end

    end

    local temp_userGroupIDSet2 = {}
    for i,v  in pairs(temp_userGroupIDSet) do
      temp_userGroupIDSet2[#temp_userGroupIDSet2+1] = i
    end

    saddTable(temp_userGroupIDSet2,dst)

end


local function genRole_GroupALL_NoPath(src,dst)


    redis.call("DEL",dst,dst .. ":Lft",dst .. ":Rgt",dst .. ":Depth")

    -- 合并所有角色直属设备对应分组，和直属分组
    unionRedisSet(src,dst)

    -- 只为了排序
    redis.call("ZINTERSTORE",dst, 2 ,dst,"Zgroup",'WEIGHTS',0,1,'AGGREGATE', 'MAX')

    --生成其它属性的集合
    redis.call("ZINTERSTORE",dst .. ":Lft", 2 ,dst,"ZgroupLft",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
    redis.call("ZINTERSTORE",dst .. ":Rgt", 2 ,dst,"ZgroupRgt",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
    redis.call("ZINTERSTORE",dst .. ":Depth", 2 ,dst,"ZgroupDepth",'WEIGHTS',0,1,'AGGREGATE', 'MAX')

  -- debug("user Group IDs ALL by direct group and device-linked-group NoPath",dst)
end





local function genRole_Group_AsTree_UnionZset(src,dst)

	local tempKeySet = getTempKey("")

	local temp_group_key = tempKeySet.tempPrefix .. "tmp-Zgroup"
	
    -- rlog("DEL " ..  dst)
    redis.call("DEL", dst,dst .. ":Lft",dst .. ":Rgt",dst .. ":Depth")
    

    
    -- 根据所有直属分组以及设备对应分组查找完整分组路径组成最终分组树集合
    local l_usr_GroupIDs_roall = redis.call("ZRANGE",src,0,-1)

    redis.call("ZUNIONSTORE",temp_group_key, 1, "ZgroupLft")

    deleteMegaFromSet(l_usr_GroupIDs_roall,temp_group_key)
       
    local l_may = redis.call("ZRANGE",temp_group_key,0,-1)
    
    local l_t_all = redis.call("ZRANGE","ZgroupLft",0,-1)
    
    local l_find = {}

    

    
    local hjk = {}
    for i,v in pairs(l_t_all) do
      local group = redis.call("HMGET","group:" .. v,"Lft","Rgt","ID")
        group[1] = group[1] + 0
        group[2]= group[2] + 0

      hjk[group[3]] = group
      
    end



    for i,vid  in pairs(l_usr_GroupIDs_roall) do
    local lft = hjk[vid][1]
    local rgt = hjk[vid][2]
    
      for m,nid in pairs(l_may) do

         local jj = hjk[nid]
          if jj[1] < lft and jj[2] > rgt then
          
            l_find[#l_find+1] = nid
   l_may[m] = nil 
          end
      end
      
    end
     
  
        saddTable(l_find,dst)
        
        redis.call("ZUNIONSTORE",dst,2,dst,src)
            
            -- 只为了排序
        redis.call("ZINTERSTORE",dst, 2 ,dst,"Zgroup",'WEIGHTS',0,1,'AGGREGATE', 'MAX')

    
    
        --生成其它属性的集合
        redis.call("ZINTERSTORE",dst .. ":Lft", 2 ,dst,"ZgroupLft",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
        redis.call("ZINTERSTORE",dst .. ":Rgt", 2 ,dst,"ZgroupRgt",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
        redis.call("ZINTERSTORE",dst .. ":Depth", 2 ,dst,"ZgroupDepth",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
    


end


local function genTempUserDevGroup(userID,role)

	-- redis.replicate_commands()
	
	local tempKeySet = getTempKey(userID)

	-- 根据用户ID记录缓存时间戳，与forceFlush比较判断是否要强制刷新
	-- 为了避免去维护记录KEY的生命周期，设定EX为300s，也就是说，缓存300s强制刷新一次
	local cacheTimeKey = tempKeySet.tempPrefix .. "user:" .. userID .. "lastDGCache"
	
	local needFlush = false
	
	local ff = redis.call("GET","forceFlush") or 1024 -- as first time
	
	local lastUt = redis.call("GET", cacheTimeKey)
	
	if ff ~= lastUt then
		needFlush = true
	end
	
	if 0 == redis.call("EXISTS", tempKeySet.key_user_drct_dev_from_role) or needFlush then
    --获取角色直属设备
    genRoleDevice_UnionSet(role,tempKeySet.key_user_drct_dev_from_role)
	end

	if 0 == redis.call("EXISTS", tempKeySet.key_user_group_from_role_drct_group) or needFlush then
    --获取角色直属分组 case A1
    genRole_InlineGroup_UnionSet(role,tempKeySet.key_user_group_from_role_drct_group)
	end

	if 0 == redis.call("EXISTS", tempKeySet.key_user_dev_from_role_group) or needFlush then
	--获取所有角色直属分组 内所有设备 case A2
	local ids = redis.call("ZRANGE", tempKeySet.key_user_group_from_role_drct_group, 0,-1)
    genRoleDevice_InlineGroup_UnionSet(ids, tempKeySet.key_user_dev_from_role_group)
	end

	if 0 == redis.call("EXISTS", tempKeySet.key_user_dev) or needFlush then
	--获取用户所有设备
    unionRedisSet({tempKeySet.key_user_dev_from_role_group,tempKeySet.key_user_drct_dev_from_role},tempKeySet.key_user_dev)
	end

	
	if 0 == redis.call("EXISTS", tempKeySet.key_user_group_from_role_drct_dev) or needFlush then
	--获取角色直属设备 对应的分组
    genRole_DeviceLinkGroup_UnionSet(tempKeySet.key_user_drct_dev_from_role,tempKeySet.key_user_group_from_role_drct_dev)
	end

	
	if 0 == redis.call("EXISTS", tempKeySet.key_user_group_no_path) or needFlush then
    --角色所有分组 包括直属和设备对应的 不包括路径上的分组
    -- 会生成 tempKeySet.key_user_group_no_path_Depth
    genRole_GroupALL_NoPath({tempKeySet.key_user_group_from_role_drct_dev,tempKeySet.key_user_group_from_role_drct_group},tempKeySet.key_user_group_no_path)
	end

	if 0 == redis.call("EXISTS", tempKeySet.key_user_group_full_path) or needFlush then
    --角色所有分组 包括直属和设备对应的 包括路径上的分组
    -- 会生成 tempKeySet.key_user_group_full_Depth 等
    genRole_Group_AsTree_UnionZset(tempKeySet.key_user_group_no_path,tempKeySet.key_user_group_full_path)
	end
	
	if ff ~= lastUt then
		redis.call("SET", cacheTimeKey, ff, "EX", 300)
	end
	
end



local function getChildGroup(parentGroupID,lv,src,ordersrc)

  local group = redis.call("HGETALL","group:" .. parentGroupID)
  if next(group) ~= nil then

    group = convertToTable(group)

    local childGroupID = {}
    local childGroup = {}

    ---[===[ 根据自己的试图从redis取出属性后在lua中计算

    --先根据层级取
    local userGrpTree = {}
    
   
    
    if(-1 == lv + 0) then
      userGrpTree = redis.call("ZRANGEBYSCORE",src,group["Depth"] + 1,'+inf')
    else
      userGrpTree = redis.call("ZRANGEBYSCORE",src,group["Depth"] + 1,group["Depth"] + lv)
    end
    
    local tempvar = "temp-getchildgroup-t0"
    redis.call("DEL",tempvar)
    saddTable(userGrpTree,tempvar)
    redis.call("ZINTERSTORE",tempvar, 2 ,tempvar,"Zgroup",'WEIGHTS',0,1,'AGGREGATE', 'MAX')
    userGrpTree = redis.call("ZRANGE",tempvar,0,-1)
    
    -- rlog("userGrpTree size " .. #userGrpTree)
    for i,v  in pairs(userGrpTree) do
      local grp = redis.call("HGETALL","group:" .. v)
--      printTab(grp)
      
      if next(grp) ~= nil then

        grp = convertToTable(grp)
        grp.id = grp.ID
        grp.name = grp.Name
        grp.ID = nil
        grp.Name = nil

        if (grp["Lft"]+0 > group["Lft"]+0) and (grp["Rgt"]+0 < group["Rgt"]+0) then
          if(grp["Lft"]+1 == grp["Rgt"]+0) then
            grp["isParent"] = "false"
          else
            grp["isParent"] = "true"
            -- 叶子节点过滤放这里
          end
          childGroupID[#childGroupID+1] = v
          childGroup[#childGroup+1] = grp
        end
      end
    end

    --	childGroup[2],childGroup[1] = childGroup[1],childGroup[2]

    --]===]


    -- rlog("child gourp level " .. lv .. " of " .. parentGroupID)
--    printTab(childGroup)

  local childGroupJson = "[]"
  if #childGroupID > 0 then
    childGroupJson = cjson.encode(childGroup)
    end

    return childGroupID,childGroupJson,childGroup,#childGroupID
  else
    -- rlog("contrast: " .. "group not found")
    return {err = "no role1111"}
  end

end


-- local userID = ARGV[1] or "13c70c07197945e1ad842f0c0e6f36cf"


-- user对应设备id集合，包括role直接对应的device和role拥有的group中的device
-- 也就是说这个用户的所有设备



-- "getGroup" userid parentGroupID lv
-- lv -1 时查所有子集
local function getUserGroup(userID, parentGroupID, lv, extra, roleID)

local tempKeySet = getTempKey(userID)

  -- rlog("getUserGroup")
  -- rlog(userID)
  -- rlog(parentGroupID)
  -- rlog(lv)
  -- rlog(extra)
  -- rlog(roleID)
  -- rlog("getUserGroup param end")

  lv = lv or 1


  local ids,json,objs,count

  if userID == "70234e42af9311e68659c598a67a71c6" then --admin
    ids,json,objs,count = getChildGroup(parentGroupID,lv,"ZgroupDepth")
  else
    --获取角色
    local role = getRoleByUserID(userID)

    if #role == 0 then
      return
    end


    genTempUserDevGroup(userID,role)

    ids,json,objs,count = getChildGroup(parentGroupID,lv,tempKeySet.key_user_group_full_Depth)



  end


    if extra == "RoleSetGroup" then

   -- printTab(objs)
      for i,v in pairs(objs) do

          local exist = redis.call("SISMEMBER","Srolegroup:"..roleID,v.id)
          if 1 == exist then
            v.checked = "true"
            v.flag = "auth"
          end          
          
      end
      
      json = cjson.encode(objs)
      
    end


  if json ~= nil then
--    -- rlog("json ")
--    -- rlog(json)
    return {json,count}
  end


end -- getUserGroup


local function getUserDeviceIDsByGroupID(userID, GroupID,lv)
local tempKeySet = getTempKey(userID)
local SubTreeAllDev = tempKeySet.tempPrefix .. "SubTreeAllDev" -- 结果集

   lv = lv or -1
  local start_pos = 0
  local end_pos = -1

  -- rlog("getUserDeviceIDsByGroupID")
  -- rlog(type(userID) .. " " .. userID)
  -- rlog(type(GroupID) .. " " .. GroupID)
  -- rlog(type(start_pos) .. " " .. start_pos)
  -- rlog(type(end_pos) .. " " .. end_pos) 
  -- rlog(type(lv) .. " " .. lv) 


  
  if userID == "70234e42af9311e68659c598a67a71c6" then --admin
      if GroupID == "00000000000000000000000000000000" then
			SubTreeAllDev = "Zdevice"
	  else

		local ids,json = getChildGroup(GroupID,lv,"ZgroupDepth")

		-- 把自己加进去
		ids[#ids+1] = GroupID

		--每次都刷新这个 是否有效率问题？
		genRoleDevice_InlineGroup_UnionSet(ids,SubTreeAllDev)

		redis.call("ZINTERSTORE",SubTreeAllDev,2,"Zdevice",SubTreeAllDev )

	  end

  else -- 非admin





    --获取角色
    local role = getRoleByUserID(userID)

    if #role == 0 then
      -- rlog("no role222")
      return tempKeySet.tempPrefix .. "emptySet"
    end


    genTempUserDevGroup(userID,role)
	

    if GroupID == "00000000000000000000000000000000" then

	  SubTreeAllDev = tempKeySet.key_user_dev

    else

      --
      local ids,json = getChildGroup(GroupID,lv,tempKeySet.key_user_group_no_path_Depth)


      -- 把自己加进去
      ids[#ids+1] = GroupID


      --每次都刷新这个，影响效率
      genRoleDevice_InlineGroup_UnionSet(ids,SubTreeAllDev)

      redis.call("ZINTERSTORE",SubTreeAllDev,2,tempKeySet.key_user_dev,SubTreeAllDev )



    end

  end
  
  		redis.call("hset", "dd", "tt", type(SubTreeAllDev))
		redis.call("hset", "dd", "ss", tostring(SubTreeAllDev))
  return SubTreeAllDev

end


-- "getDevice" userid GroupID pageNumber,pageSize CriteriaName
local function getUserDevice(userID, GroupID,pageNumber,pageSize, lv,
 c_name , c_nmsid, c_netAddress , c_mainTypeID, c_childTypeID, c_agentID, c_OnlineStatus,
 c_domainName,c_modelInfo,c_factoryName,c_limit,c_version,c_protocol, c_cap)
 
 -- /opt/kdm//system/script/getFromRedis.lua getUserDevice 70234e42af9311e68659c598a67a71c6 00000000000000000000000000000000 1 15 -1
 

  local SubTreeAllDev =  getUserDeviceIDsByGroupID(userID, GroupID,lv)
 
 
  local total, groupDevIDs
  
  local tp = redis.call("TYPE",SubTreeAllDev )
  
  if "zset" == tp.ok then
     total = redis.call("ZCARD",SubTreeAllDev)

   groupDevIDs = redis.call("ZRANGE",SubTreeAllDev,0,-1)
  else -- set or none
   total = redis.call("SCARD",SubTreeAllDev)

   groupDevIDs = redis.call("SMEMBERS",SubTreeAllDev)
  
  end
 

  
  local resultIDs = {}

  local result = {}
  
  local q_fields = {}
  
  
  local flag_name = 0
  if (c_name and string.len(c_name) > 0) then 
    flag_name = 1 
    c_name = string.lower(c_name)
	q_fields.Name = #q_fields+1
	q_fields[#q_fields+1] = "Name"
  end
  
  local flag_nmsid = 0
  if (c_nmsid and string.len(c_nmsid) > 0) then
	flag_nmsid = 1 
  	q_fields.ID = #q_fields+1
	q_fields[#q_fields+1] = "ID"
  end
  
  local flag_netAddress = 0
  if (c_netAddress and string.len(c_netAddress) > 0) then 
	flag_netAddress = 1 
	q_fields.NetAddress = #q_fields+1
	q_fields[#q_fields+1] = "NetAddress"
  end
  
  local flag_mainTypeID = 0
  if (c_mainTypeID and string.len(c_mainTypeID) > 0) then 
    flag_mainTypeID = 1 
	q_fields.MainTypeID = #q_fields+1
	q_fields[#q_fields+1] = "MainTypeID"
  end
  
  local flag_childTypeID = 0
  if (c_childTypeID and string.len(c_childTypeID) > 0) then
    flag_childTypeID = 1 
	q_fields.ChildTypeID = #q_fields+1
	q_fields[#q_fields+1] = "ChildTypeID"
  end
  
  local flag_agentID = 0
  local c_agentName = nil
  if (c_agentID and string.len(c_agentID) > 0) then
    flag_agentID = 1 
	q_fields.AgentID = #q_fields+1
	q_fields[#q_fields+1] = "AgentID"
--    c_agentName = redis.call("HGET","device:"..c_agentID,"Name")
  end
  
  local flag_OnlineStatus = 0
  if (c_OnlineStatus and string.len(c_OnlineStatus) > 0) then 
    flag_OnlineStatus = 1 
	q_fields.OnlineStatus = #q_fields+1
	q_fields[#q_fields+1] = "OnlineStatus"
  end  
  
--  c_domainName = "201-41"
  
  local flag_domainName = 0
  local c_domainID = nil
  if (c_domainName and string.len(c_domainName) > 0) then
   flag_domainName = 1 
   q_fields.DomainID = #q_fields+1
   q_fields[#q_fields+1] = "DomainID"
   
   c_domainID = redis.call("HGET","domain",c_domainName)
     if not c_domainID then
        c_domainID = ""
     end
   end  
  
  local flag_modelInfo = 0
  if (c_modelInfo and string.len(c_modelInfo) > 0) then 
    flag_modelInfo = 1 
	q_fields.ModelInfo = #q_fields+1
	q_fields[#q_fields+1] = "ModelInfo"
  end
  
  local flag_factoryName = 0
  if (c_factoryName and string.len(c_factoryName) > 0) then
    flag_factoryName = 1 
	q_fields.DevFactoryName = #q_fields+1
	q_fields[#q_fields+1] = "DevFactoryName"
  end 
 
  local flag_limit = 0
  if (c_limit and (c_limit+0) > 0) then flag_limit = 1 end
  
  local flag_version = 0
  if (c_version and string.len(c_version) > 0) then
    flag_version = 1 
	c_version = string.lower(c_version)
	q_fields.Version = #q_fields+1
	q_fields[#q_fields+1] = "Version"
  end

  local flag_protocol = 0
  if (c_protocol and string.len(c_protocol) > 0) then
    flag_protocol = 1 
	c_protocol = string.lower(c_protocol)
	q_fields.ProtocolName = #q_fields+1
	q_fields[#q_fields+1] = "ProtocolName"
  end
  
  local flag_cap = 0
  if (c_cap and string.len(c_cap) > 0) then
    flag_cap = 1 
	c_cap = string.lower(c_cap)
	q_fields.Capability = #q_fields+1
	q_fields[#q_fields+1] = "Capability"
  end
  
  
  local flag_condtion_total = flag_name + flag_nmsid + flag_netAddress
                               + flag_mainTypeID + flag_childTypeID + flag_agentID 
							   + flag_OnlineStatus + flag_domainName + flag_modelInfo + flag_factoryName 
							   + flag_version + flag_protocol + flag_cap;
  
  
  
  if flag_condtion_total > 0 then -- 需要过滤
  -- rlog("device filter")
  total = 0



  local flag_math = 0
  
  for i,v  in pairs(groupDevIDs) do

--    local device = redis.call("HGETALL","device:" .. v)
    local m_dev = redis.call("HMGET","device:" .. v, unpack(q_fields))

--    device = convertToTable(device)

--    if not m_dev[q_fields.DomainID] then
--        m_dev[q_fields.DomainID] = redis.call("HGET","device:" .. m_dev[q_fields.AgentID],"DomainID")
--        if m_dev[q_fields.DomainID] then
--           redis.call("HSET","device:" .. v,"DomainID",m_dev[q_fields.DomainID])
--        end
--    end


    flag_math = 0
    
    if flag_name > 0 and string.find(string.lower(m_dev[q_fields.Name]), c_name,1,true) then
      flag_math = flag_math + 1
    end

    if flag_nmsid > 0 and string.find(m_dev[q_fields.ID], c_nmsid,1,true) then
      flag_math = flag_math + 1
    end

    if flag_netAddress > 0 and m_dev[q_fields.NetAddress] and string.find(m_dev[q_fields.NetAddress], c_netAddress,1,true) then
      flag_math = flag_math + 1
    end
    
    if flag_mainTypeID > 0 and m_dev[q_fields.MainTypeID] == c_mainTypeID then
      flag_math = flag_math + 1
    end
    
    if flag_childTypeID > 0 and string.find(c_childTypeID, '%('..m_dev[q_fields.ChildTypeID]..'%)') then
      flag_math = flag_math + 1
    end
    
    if flag_agentID > 0 and m_dev[q_fields.AgentID] == c_agentID then
--      device.AgentName = c_agentName
      flag_math = flag_math + 1
    end
    
    if flag_OnlineStatus > 0 and ((not m_dev[q_fields.OnlineStatus] and c_OnlineStatus == "0") or (m_dev[q_fields.OnlineStatus] == c_OnlineStatus)) then
      flag_math = flag_math + 1
    end
    
    if flag_domainName > 0 and m_dev[q_fields.DomainID] == c_domainID then
--      device.DomainName = c_domainName
      flag_math = flag_math + 1
    end
    
    if flag_modelInfo > 0 and m_dev[q_fields.ModelInfo] == c_modelInfo then
      flag_math = flag_math + 1
    end
    
    if flag_factoryName > 0 and m_dev[q_fields.DevFactoryName] == c_factoryName then
      flag_math = flag_math + 1
    end	
	
    if flag_version > 0 and m_dev[q_fields.Version] and string.find(string.lower(m_dev[q_fields.Version]), c_version,1,true) then
      flag_math = flag_math + 1
    end

    if flag_protocol > 0 then
		local pro = string.lower( '%('.. tostring(m_dev[q_fields.ProtocolName]) ..'%)' )
		if string.find(c_protocol, pro ) then
		  flag_math = flag_math + 1
		end
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
				if  not string.find(temp_cap,  string.lower(w) ,1,true)  then
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
	  
	  if flag_limit > 0 and total > c_limit + 0 then
		return  {-c_limit}
	  end
	  
      if total > (pageNumber-1)*pageSize and total <= pageNumber*pageSize then
            --      -- rlog(device["Name"])

      local device = redis.call("HGETALL","device:" .. v)
      device = convertToTable(device)      
      
      result[#result+1] = device
      resultIDs[#resultIDs+1] = v
      --       -- rlog("cjson " .. cjson.encode(device))
      end
    end
    
  end
  else
  -- rlog("device no filter")
  
  	  if flag_limit > 0 and total > c_limit + 0 then
		return  {-c_limit}
	  end
  
  
       for i=(pageNumber-1)*pageSize+1,math.min(pageNumber*pageSize,total)  do
          local device = redis.call("HGETALL","device:" .. groupDevIDs[i])
          device = convertToTable(device)
          result[#result+1] = device
          resultIDs[#resultIDs+1] = groupDevIDs[i]
      end
  end

  
  
  --添加agentName 和 domainName
  local t_agentNames = {}
  local t_domainNames = {}
  local t_dauNames = {}
  
  for i,v in pairs(result) do 
      if v.AgentID then
        if not t_agentNames[v.AgentID] then
          t_agentNames[v.AgentID] = redis.call("HGET","device:"..v.AgentID,"Name")
        end
        
        if t_agentNames[v.AgentID] then
           v.AgentName = t_agentNames[v.AgentID];
        end
        
      end
  
      if v.DomainID then
        if not t_domainNames[v.DomainID] then
          t_domainNames[v.DomainID] = redis.call("HGET","domain:"..v.DomainID,"Name")
        end
        
        if t_domainNames[v.DomainID] then
           v.DomainName = t_domainNames[v.DomainID];
        end
        
      end  
  
      if v.DauID then
        if not t_dauNames[v.DauID] then
          t_dauNames[v.DauID] = redis.call("HGET","dau:"..v.DauID,"Name")
        end
        
        if t_dauNames[v.DauID] then
           v.DauName = t_dauNames[v.DauID];
        end
        
      end  
  
    if v.ActualIP == "None" then
        v.ActualIP = nil;
    end
  end


  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  local rspp = {total,#result,resultIDs,result_json}
  
  return  rspp

end

local function countOnlineByGroup(userID,GroupID,lv)
  local tempKeySet = getTempKey(userID)
  local SubTreeAllDev_temp = tempKeySet.tempPrefix .. "SubTreeAllDev_temp_co"
  
  local SubTreeAllDev = getUserDeviceIDsByGroupID(userID, GroupID,lv)
  
  
  local tp = redis.call("TYPE",SubTreeAllDev )
  
  if "zset" == tp.ok then
  
  local allCount = redis.call("ZCARD", SubTreeAllDev)
  
  redis.call("ZINTERSTORE", SubTreeAllDev_temp , 2, SubTreeAllDev, "onlineDevice")

    local onlineCount = redis.call("ZCARD", SubTreeAllDev_temp)
    
    return {GroupID,onlineCount,allCount}
 else -- set or none
   local allCount = redis.call("SCARD", SubTreeAllDev)
  
  redis.call("SINTERSTORE", SubTreeAllDev_temp , SubTreeAllDev, "onlineDevice")

    local onlineCount = redis.call("SCARD", SubTreeAllDev_temp)
    
    return {GroupID,onlineCount,allCount}
 
 end
	
end


local function searchGroupByName(userID, namePattern)

local tempKeySet = getTempKey(userID)

  -- rlog("searchGroupByName")
  -- rlog(userID)
  -- rlog(namePattern)
  -- rlog("searchGroupByName param end")

  namePattern = string.lower(namePattern)


  local l_usr_GroupIDs_search = {} 
  local resultSet = {}
  local resultJson = "[]"
  if userID == "70234e42af9311e68659c598a67a71c6" then --admin
    l_usr_GroupIDs_search = redis.call("ZRANGE","ZgroupDepth",0,-1)
  else
    --获取角色
    local role = getRoleByUserID(userID)

    if #role == 0 then
      return {resultJson,#resultSet}
    end



    genTempUserDevGroup(userID,role)
    
    l_usr_GroupIDs_search = redis.call("ZRANGE",tempKeySet.key_user_group_full_path,0,-1)
    
  end



    local r_searchGroup_temp = tempKeySet.tempPrefix .. "temp-searchGroup-byName"
    local l_match = {}
    local l_match_2 = {}
    
    for i,v  in pairs(l_usr_GroupIDs_search) do

      local groupName = redis.call("HGET","group:" .. v,"Name")
      if groupName and string.find(string.lower(groupName), namePattern,1,true) then
           l_match[#l_match+1] = v   
           l_match_2[v] = 1
      end
    end
  
    redis.call("DEL",r_searchGroup_temp)
    saddTable(l_match,r_searchGroup_temp)
    -- covert to zset
    redis.call("ZUNIONSTORE",r_searchGroup_temp,1,r_searchGroup_temp)
    
    --角色所有分组 包括直属和设备对应的 包括路径上的分组
    -- 会生成 xxx_Depth 等
    
    local r_usr_grpIDs_search_tree_temp = tempKeySet.tempPrefix .. "temp-searchGroup-tree-byName"
    genRole_Group_AsTree_UnionZset(r_searchGroup_temp,r_usr_grpIDs_search_tree_temp)
    
    
    
    local l_match_path = redis.call("ZRANGE",r_usr_grpIDs_search_tree_temp,0,-1)
    
    local temp_result = {}
    for i,v in pairs(l_match_path) do
      local group = redis.call("HGETALL","group:" .. v)
      if next(group) ~= nil then
          group = convertToTable(group)
          temp_result[#temp_result+1] = group
      end
    
    end
    
    local t_flag = false
    -- rlog("temp_result size " .. #temp_result)
    for i,group in pairs(temp_result) do
        if(l_match_2[group.ID]) then  -- 是匹配分组
--            -- rlog("find " .. group.Name)
            group.flag="bingo"
            
            t_flag = false
            if group.Lft + 1 ~= group.Rgt + 0 then
                for m,n  in pairs(temp_result) do
                    if n.Lft + 0 > group.Lft + 0 and n.Rgt + 0 < group.Rgt + 0 then
                        t_flag = true
                        break
                    end
                end
                if not t_flag then
                    group.flag="bingoTail"
                end
              
            end
            
        end
       
        
        group.id = group.ID
        group.name = group.Name
        group.ID = nil
        group.Name = nil
        resultSet[#resultSet+1] = group
    end
    


  if #resultSet >0 then
    resultJson = cjson.encode(resultSet)
  end
  -- rlog(#resultSet)
--    -- rlog("search group resultJson")
--    -- rlog(resultJson)
  return {resultJson,#resultSet}

end

local function searchGroupByID(userID, idPattern)

local tempKeySet = getTempKey(userID)

  -- rlog("searchGroupByName")
  -- rlog(userID)
  -- rlog(idPattern)
  -- rlog("searchGroupByName param end")

  idPattern = string.lower(idPattern)


  local l_usr_GroupIDs_search = {} 
  local resultSet = {}
  local resultJson = "[]"
  if userID == "70234e42af9311e68659c598a67a71c6" then --admin
    l_usr_GroupIDs_search = redis.call("ZRANGE","ZgroupDepth",0,-1)
  else
    --获取角色
    local role = getRoleByUserID(userID)

    if #role == 0 then
      return {resultJson,#resultSet}
    end

	genTempUserDevGroup(userID,role)
	
    l_usr_GroupIDs_search = redis.call("ZRANGE",tempKeySet.key_user_group_full_path,0,-1)
    
  end



    local r_searchGroup_temp = tempKeySet.tempPrefix .. "temp-searchGroup-byID"
    local l_match = {}
    local l_match_2 = {}
    
    for i,v  in pairs(l_usr_GroupIDs_search) do

      local groupName = redis.call("HGET","group:" .. v,"Name")
      if groupName and string.find(string.lower(v), idPattern,1,true) then
           l_match[#l_match+1] = v   
           l_match_2[v] = 1
      end
    end
  
    redis.call("DEL",r_searchGroup_temp)
    saddTable(l_match,r_searchGroup_temp)
    -- covert to zset
    redis.call("ZUNIONSTORE",r_searchGroup_temp,1,r_searchGroup_temp)
    
    --角色所有分组 包括直属和设备对应的 包括路径上的分组
    -- 会生成 xxx_Depth 等
    
    local r_usr_grpIDs_search_tree_temp = tempKeySet.tempPrefix .. "temp-searchGroup-tree-byID"
    genRole_Group_AsTree_UnionZset(r_searchGroup_temp,r_usr_grpIDs_search_tree_temp)
    
    
    
    local l_match_path = redis.call("ZRANGE",r_usr_grpIDs_search_tree_temp,0,-1)
    
    local temp_result = {}
    for i,v in pairs(l_match_path) do
      local group = redis.call("HGETALL","group:" .. v)
      if next(group) ~= nil then
          group = convertToTable(group)
          temp_result[#temp_result+1] = group
      end
    
    end
    
    local t_flag = false
    -- rlog("temp_result size " .. #temp_result)
    for i,group in pairs(temp_result) do
        if(l_match_2[group.ID]) then  -- 是匹配分组
--            -- rlog("find " .. group.Name)
            group.flag="bingo2"
            
            t_flag = false
            if group.Lft + 1 ~= group.Rgt + 0 then
                for m,n  in pairs(temp_result) do
                    if n.Lft + 0 > group.Lft + 0 and n.Rgt + 0 < group.Rgt + 0 then
                        t_flag = true
                        break
                    end
                end
                if not t_flag then
                    group.flag="bingoTail2"
                end
              
            end
            
        end
       
        
        group.id = group.ID
        group.name = group.Name
        group.ID = nil
        group.Name = nil
        resultSet[#resultSet+1] = group
    end
    


  if #resultSet >0 then
    resultJson = cjson.encode(resultSet)
  end
  -- rlog(#resultSet)
--    -- rlog("search group resultJson")
--    -- rlog(resultJson)
  return {resultJson,#resultSet}

end


--local parentGroupID = "00000000000000000000000000000000"
-- parentGroupID = "0e3cfbb4d74011e68cd7000c29e55fab"
--getUserGroup(userID,parentGroupID)
--getUserDevice(userID,parentGroupID)


local function deviceRegister(regTime,dauID, ...)

local setSize = 7

-- rlog("enter deviceRegister")
local isDauOnline = "1" == redis.call("HGET","dau:"..dauID,"OnlineStatus")
local isDefaultDau = "1" == redis.call("HGET","dau:"..dauID,"IsDefault")

local kdmnos = {}
local agentID_list = {}

for i = 1,arg.n,setSize do
   agentID_list[#agentID_list+1] = arg[i]
   kdmnos[#kdmnos+1] = arg[i+1]
end

--local deviceIDs = redis.call("HMGET","agentkdmno:"..agentID,unpack(kdmnos))

  local result = {}
  local t_protoIDs = {}
  
for i,kno in ipairs(kdmnos) do
local agentID = agentID_list[i]
local useAgent = agentID and kno and kno ~= agentID

local dev_id = nil
-- agnetid 跟kdmno  相同说明不走代理，直接校验私有id
if agentID and agentID == kno then
  dev_id = kno
else
  dev_id = redis.call("HGET","agentkdmno:"..agentID,kno)
end
  if dev_id then -- 找到了设备id
  local dev_r_key = "device:" .. dev_id
--    -- rlog(v)
      local device = redis.call("HGETALL",dev_r_key)
      if next(device) ~= nil then
      device = convertToTable(device)
      
      repeat
      if not isDauOnline then --dau 不在线
        result[#result+1] = {RegErr = "DauNotOnline", KDMNO=kno, AgentID=agentID}
        break
      end
      
    if useAgent then
      if device["MainTypeID"] ~= "4" then
        local devdata_t = redis.call("HMGET","device:"..device["AgentID"],"OnlineStatus","DauID")
        if "1" ~= devdata_t[1] then --agent 不在线
          result[#result+1] = {RegErr = "AgentNotOnline", KDMNO=kno, AgentID=agentID}
          break
        end
        
        local daucheck_f = false
        if devdata_t[2] and "1" == redis.call("HGET","dau:"..devdata_t[2],"OnlineStatus") and devdata_t[2] == dauID then
          daucheck_f = true
        end
        
        if false == daucheck_f then
          result[#result+1] = {RegErr = "DauNotMatch", KDMNO=kno, AgentID=agentID}
          break
        end
      end
    end
      
      if "1" ~= device["UsageStatus"] then --禁用
        result[#result+1] = {RegErr = "Prohibited", KDMNO=kno, AgentID=agentID}
        break
      end
      
      if "1" == device["OnlineStatus"] then --agent 已经在线
        result[#result+1] = {RegErr = "AlreadyOnline", KDMNO=kno, AgentID=agentID}
        break
      end      
      
      local expTime = tonumber(device["ExpireDate"]) or 0
      if regTime+0 > expTime then
        result[#result+1] = {RegErr = "Expired", KDMNO=kno, AgentID=agentID}
        break
      end
      
      

      local protoName = arg[(i-1)*setSize+3]
      -- rlog( i .. " protoName " .. protoName)
      if not t_protoIDs[protoName] then
          -- rlog("get protoid")
          t_protoIDs[protoName] = redis.call("HGET","protocol",protoName)
      end
      
      if "snmp" == protoName and isDefaultDau =="0" and device["MainTypeID"] == "5" then
          result[#result+1] = {RegErr = "NotDefaultDau", KDMNO=kno, AgentID=agentID}
          break
      end
      
      
      local oldprotoid = device["AccProtocol"]
        
      if t_protoIDs[protoName] then
       -- rlog("set protoid")
           redis.call("HSET",dev_r_key,"AccProtocol",t_protoIDs[protoName])
           redis.call("HSET",dev_r_key,"ProtocolName",protoName)
      else
        result[#result+1] = {RegErr = "ProtocolNotFound", KDMNO=kno, AgentID=agentID}
        break
      end
      
      redis.call("HSET",dev_r_key,"DauID",dauID)
      
      
      local aip = arg[(i-1)*setSize+4]
      local cap = arg[(i-1)*setSize+5]
      local ver = arg[(i-1)*setSize+6]
      local hardVer = arg[(i-1)*setSize+7]
      
      if (not ver or ver == "") then
        ver = device.Version or ''
      end
      
      if (not hardVer or hardVer == "") then
        hardVer = device.HardVersion or ''
      end
      
      redis.call("HMSET",dev_r_key,"ActualIP",aip,"RegTime",regTime,"OnlineStatus",1,"Capability",cap,"Version",ver,"HardVersion",hardVer)
	  redis.call("SADD","onlineDevice", dev_id)
--      redis.call("ZADD","Zdevice","XX",100,dev_id)
      
      redis.call("SADD","daudevice:" .. dauID,dev_id) -- 记录dau 和在线 device 关系
    
    if useAgent then
          redis.call("SADD","agentOnlineDevice:" .. agentID,dev_id) -- 记录agent 和在线 device 关系
      end
      
      if oldprotoid ~= t_protoIDs[protoName] and tonumber(oldprotoid) ~= tonumber(t_protoIDs[protoName]) then
          device.UpdateProtocol = "1"
          device.AccProtocol = t_protoIDs[protoName]
          device.ProtocolName = protoName
      end
      
      if ver and "" ~= ver and 
	  ((not device.Version or "None" == device.Version or "" == device.Version) or (device.Version and "" ~= device.Version and device.Version ~= ver) )
		then
        device.UpdateVersion = "1"
      end
      -- make sure DevUpdateLog updated even if version not change after devupdate,cost performance degradation 
	  device.UpdateVersion = "1"
	  
      device.Version = ver
      if ver and "None" == tostring(ver) then
		device.Version = ""
      end
	  
	  
      result[#result+1] = device
      
      until true
      
      else
        result[#result+1] = {RegErr = "NotFound", KDMNO=kno, AgentID=agentID}
      
      end
  else
      result[#result+1] = {RegErr = "NotFound", KDMNO=kno, AgentID=agentID}
  end

  
end
  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {result_json}
end


local function getNMSIDByKDMNO(...)

    -- rlog("enter getNMSIDByKDMNO")
    
    local ret = {}
    for i = 1,arg.n,2 do
       local c = {}
       c.AgentID = arg[i]
       c.KDMNO = arg[i+1]
       c.Result = "NotFound";
       ret[#ret+1]=c
    end
    
    for i,v in ipairs(ret) do
        local id = redis.call("HGET","agentkdmno:".. v.AgentID , v.KDMNO)
        if id then
          local devdata = redis.call("HMGET","device:".. id , "Name","ActualIP","ChildTypeName")
          v.ID = id
          
          if devdata[1] and "None" ~= devdata[1] then
              v.Name = devdata[1]
          else
              v.Name = ""
          end
          
          if devdata[2] and "None" ~= devdata[2] then
              v.ActualIP = devdata[2]
          else
              v.ActualIP = ""
          end
          
          if devdata[3] and "None" ~= devdata[3] then
              v.ChildTypeName = devdata[3]
          else
              v.ChildTypeName = ""
          end
          
          v.Result = "OK"
        end
    end

    local json = "[]"
    
    if #ret > 0 then
       json = cjson.encode(ret)
    end

    return {json}

end


local function setAlarmStat(json_param )
-- rlog("enter setAlarmStat")
-- [ {'ID' = "" , "AgentID" = "" , "KDMNO" = "", "STATUS" = ""} ]
  local paramArr = cjson.decode(json_param)

  
  for i,param in ipairs(paramArr) do

    local deviceID = nil
    if param.ID then
      deviceID = param.ID
    else
        deviceID = redis.call("HGET","agentkdmno:"..param.AgentID,param.KDMNO)
    end
  
    if deviceID and redis.call("EXISTS",'device:' .. deviceID) > 0 then
          redis.call("HSET",'device:' .. deviceID,"AlarmStatus",param.STATUS)    
          paramArr[i].RESULT="OK"
    else
          paramArr[i].RESULT="NotFound"
    end
    
  
  end
  
local json = cjson.encode(paramArr)

return {json}

end


local function updateDevUniqProp(json_param )
-- rlog("enter updateDevUniqProp")

  local paramArr = cjson.decode(json_param)

  if type(paramArr.devProp) == "table" then
	
     for i,param in ipairs(paramArr.devProp) do
        if param.tableName == "Device" then
	  if param.deviceID and redis.call("EXISTS", 'device:' .. param.deviceID) > 0 then
             if param.version and string.len(param.version) > 0 then
               redis.call("HSET", 'device:' .. param.deviceID, 'Version',  param.version) 
             end
          end
        elseif param.tableName == "DevUniqProp" then      
          if param.deviceID and redis.call("EXISTS", 'device:' .. param.deviceID) > 0 then
             if param.modelInfo and string.len(param.modelInfo) > 0 then
               redis.call("HSET", 'device:' .. param.deviceID, 'ModelInfo',  param.modelInfo)
             end
          end 
        end
     end

  end
  
return {}

end


local function deviceOffline(mode,dauID,...)

-- rlog("enter deviceOffline")
local deviceIDs = {}
if "nms_id" == mode then
  for i = 1,arg.n do
     deviceIDs[#deviceIDs+1] = arg[i]
  end
elseif "agent_kdmno" == mode then
  for i = 1,arg.n,2 do
    local agentid = arg[i]
    local kdmno = arg[i+1]
    if agentid then
      if agentid == kdmno then
         deviceIDs[#deviceIDs+1] = agentid
      else
         local devid = redis.call("HGET","agentkdmno:"..agentid,kdmno)
         if devid then
            deviceIDs[#deviceIDs+1] = devid
         end
      end
    end
  end
  
  
  
  
end

  local result = {}
  
for i,v in ipairs(deviceIDs) do

  if v then -- 找到了设备id
  -- rlog("dev " .. v)
      local device = redis.call("HGETALL","device:" .. v)
      if next(device) ~= nil then

      repeat
      
      device = convertToTable(device)
      
      local agentID = device.AgentID
      
      result[#result+1] = device
    
      if device["MainTypeID"] == "4"  then --agent 下线
        local agentOLdev = redis.call("SMEMBERS","agentOnlineDevice:" .. agentID)
        redis.call("DEL","agentOnlineDevice:" .. agentID)
        deleteMegaFromSet(agentOLdev,"daudevice:" .. dauID)
        
        for i,v in pairs(agentOLdev) do
            redis.call("HSET","device:" .. v,"OnlineStatus",0)
        end
        
      end
    redis.call("SREM","daudevice:" .. dauID,v)
    redis.call("SREM","agentOnlineDevice:" .. agentID,v)
    redis.call("HSET","device:" .. v,"OnlineStatus",0)
	
	redis.call("SREM","onlineDevice", v)
	
--    redis.call("ZADD","Zdevice","XX",999,v)
    
        --区分是agent退网还是设备退网
      
      until true
      
      end
      

  else
      result[#result+1] = {RegErr = "NotFound"}
  end

  end
  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {result_json}
end

local function dauRegister(dauID,session,actualIP,regTime,uptime,ver,hardVer, cap)

cap = cap or ""

-- rlog("enter dauRegister")
local result = {}
local dau = redis.call("HGETALL","dau:" .. dauID)
if next(dau) ~= nil then
        dau = convertToTable(dau)

repeat

        if dau.UsageStatus == "0" then
            result.RegDauRsp = "DauDiabled"
            break 
        end

        if dau.OnlineStatus == "1" then
            result.RegDauRsp = "DauAlreadyOnline"
            break
        end

redis.call("HMSET","dau:" .. dauID,"OnlineStatus",1,"Session",session,"ActualIP",actualIP,"RegTime",regTime,"HeartBeat",uptime,"Version",ver,"HardVersion",hardVer,"Capability",cap)
dau.RegDauRsp = "OK"
result = dau
until true

else
        result.RegDauRsp = "DauNotFound"
end

local result_json = cjson.encode(result)
return  {result_json}

end

local function dauOffline(dauID)
redis.replicate_commands()
-- rlog("enter dauOffline")
local result = {}
local dau = redis.call("HGETALL","dau:" .. dauID)
if next(dau) ~= nil then
      local alarmList = {}
      local ct = redis.call('TIME')
      redis.call("HDEL","dau:"..dauID,"HeartBeat")
      redis.call("HSET","dau:"..dauID,"OnlineStatus",0)
      local daudeviceids = redis.call("SMEMBERS","daudevice:" .. dauID)
      redis.call("DEL","daudevice:" .. dauID)
      for i,v in pairs(daudeviceids) do
        redis.call("HSET","device:" .. v,"OnlineStatus",0)
		
		redis.call("SREM","onlineDevice", v)
		
        local devdata = redis.call("HMGET","device:" .. v,"DauID","AgentID","KDMNO")
        
        local alarm = {}
        alarm.m_dwAlarmCode = 10004
        alarm.m_dwChannel = 0
        alarm.m_dwAlarmFlag = 1
        alarm.m_dwAlarmTime = ct[1]
        alarm.m_strDauNo = devdata[1]
        alarm.m_strAgentID = devdata[2]
        alarm.m_strDeviceNo = devdata[3]
        alarm.m_strDesc = "offline_dmu"
        alarmList[#alarmList+1] = alarm
        
  --      [{m_dwAlarmCode:1,m_dwChannel:1,m_dwAlarmFlag:1,m_dwAlarmTime:11000,m_strDauNo:1,m_strAgentID:1,m_strDeviceNo:1,m_strDesc:1}]
      end
      
      if #alarmList > 0 then
        redis.call("RPUSH","AlarmNtf",cjson.encode(alarmList))
      end
      
      result.RegDauRsp = "OK"
else
        result.RegDauRsp = "DauNotFound"
end

local result_json = cjson.encode(result)
return  {result_json}

end


local function getDau(dauID)

-- rlog("enter getDau")
local dau = redis.call("HGETALL","dau:" .. dauID)
if next(dau) ~= nil then
        dau = convertToTable(dau)
        dau.Session = dau.Session or "-1"
        return {"OK",cjson.encode(dau)}
end

return  {"ERROR"}

end


local function getAllDau(stat,agentID, cap)

-- rlog("enter getAllDau")
agentID = agentID or "AgentIDNull"

local result = {}
local dauset = redis.call("SMEMBERS","dauset")
if dauset then
      for i,v in pairs(dauset) do
      local dau = redis.call("HGETALL","dau:" .. v)
        dau = convertToTable(dau)
        dau.Session = dau.Session or "-1"
        
        repeat
        if stat == "online" and (not dau.OnlineStatus or dau.OnlineStatus == "0") then
          break
        end
        
        if stat == "offline" and dau.OnlineStatus == "1" then
          break
        end
        
		if cap then
			if not dau.Capability then
				break 
			end
		
			local temp_cap = string.lower(dau.Capability)
		
			local it = string.gmatch(cap,"%(%w+%)")
			
			if not it() then
				break
			else
				it = string.gmatch(cap,"%(%w+%)")
			end
			
			local flag_cap = false
			for w in it do 
				if  not string.find(temp_cap,  string.lower(w) ,1,true)  then
					flag_cap = true
					break
				end
			end
			
			if flag_cap then
				break
			end
			
			
		end
        
        if agentID ~= "AgentIDNull" then
            local ex = redis.call("SISMEMBER","daudevice:"..dau.ID,agentID)
            if(ex == 1) then 
            dau.Flag = "pathDau"
            end

        end
        
        
        result[#result+1] = dau
        
        until true
        
      end
end

  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  
  -- rlog(result_json)
return  {#result,result_json}

end




local function freshDauHeartBeat(dauID,uptime)

-- rlog("enter freshDauHeartBeat")
local dau = redis.call("HGETALL","dau:" .. dauID)
if next(dau) ~= nil then
        dau = convertToTable(dau)
        
        if "1" == dau.OnlineStatus then
            redis.call("HSET","dau:" .. dauID,"HeartBeat",uptime)
            dau.HeartBeat = uptime
        end
        return {"OK",cjson.encode(dau)}
end

return  {"ERROR"}

end

local function getDeviceByIDs(...)
local devIDs = {}
for i = 1,arg.n do
   devIDs[#devIDs+1] = arg[i]
end


  local result = {}

  local t_agentNames = {}
  local t_domainNames = {}
  local t_dauNames = {}

  for i,id  in pairs(devIDs) do

    local v = redis.call("HGETALL","device:" .. id)
    if next(v) ~= nil then
    v = convertToTable(v)
    
      if v.AgentID then
      if not t_agentNames[v.AgentID] then
        t_agentNames[v.AgentID] = redis.call("HGET","device:"..v.AgentID,"Name")
      end
      
      if t_agentNames[v.AgentID] then
         v.AgentName = t_agentNames[v.AgentID];
      end
      
      end
    
      if v.DomainID then
      if not t_domainNames[v.DomainID] then
        t_domainNames[v.DomainID] = redis.call("HGET","domain:"..v.DomainID,"Name")
      end
      
      if t_domainNames[v.DomainID] then
         v.DomainName = t_domainNames[v.DomainID];
      end
      
      end  
    
      if v.DauID then
      if not t_dauNames[v.DauID] then
        t_dauNames[v.DauID] = redis.call("HGET","dau:"..v.DauID,"Name")
      end
      
      if t_dauNames[v.DauID] then
         v.DauName = t_dauNames[v.DauID];
      end
      
      end  
     

    if v.ActualIP == "None" then
      v.ActualIP = nil;
    end
    
	if v.Version == "None" then
      v.Version = "";
    end
	
	
    result[#result+1] = v
    
  end
  end

  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {#result,devIDs,result_json}

end

local function getDevUpdateInfoByIDs(...)
local devIDs = {}
for i = 1,arg.n do
   devIDs[#devIDs+1] = arg[i]
end

  local result = {}

  for i,id  in pairs(devIDs) do

	local v={}
	
	v["ID"]=id
	
	if redis.call("EXISTS", 'device:' .. id) > 0 then
	
		local ver = redis.call("HGET","device:" .. id,"Version")
		
		if ver == "None" then
			ver=""
		end
		
		v["Version"]=ver
		
		local onlineStatus = redis.call("HGET","device:" .. id,"OnlineStatus")
		
		if onlineStatus == "None" then
			onlineStatus=""
		end
		
		v["OnlineStatus"]=onlineStatus
		
		local actualIP = redis.call("HGET","device:" .. id,"ActualIP")
		
		if actualIP == "None" then
			actualIP = ""
		end
		
		v["ActualIP"]=actualIP
		
		local modelInfo = redis.call("HGET","device:" .. id,"ModelInfo")
		
		if modelInfo == "None" then
			modelInfo = ""
		end
		
		v["ModelInfo"]=modelInfo
		
		local regTime = redis.call("HGET","device:" .. id,"RegTime")
		
		if regTime == "None" then
			regTime = 0
		end
		
		v["RegTime"]=regTime
		

	else
		v["Version"] = ""
		v["OnlineStatus"] = 0
		v["ActualIP"]=""
		v["ModelInfo"]=""
		v["RegTime"]=0
	end
	
     
    result[#result+1] = v
  end

  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {#result,devIDs,result_json}

end




local function getDauInfoByIDs(...)
local devIDs = {}
for i = 1,arg.n do
   devIDs[#devIDs+1] = arg[i]
end


  local result = {}

  local t_agentNames = {}
  local t_domainNames = {}
  local t_dauNames = {}

  for i,id  in pairs(devIDs) do

    local _dau = redis.call("HGETALL","dau:" .. id)
  if next(_dau) ~= nil then
    _dau = convertToTable(_dau)
    
    if _dau.ActualIP == "None" then
      _dau.ActualIP = nil;
    end
    
    _dau.OnlineDevCount = redis.call("SCARD","daudevice:" .. id)
    
    result[#result+1] = _dau
  end
  end

  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {#result,devIDs,result_json}

end







local function getAgentStatus(...)
local devIDs = {}
for i = 1,arg.n do
   devIDs[#devIDs+1] = arg[i]
end


  local result = {}

  local t_agentNames = {}
  local t_domainNames = {}
  local t_dauNames = {}

  for i,id  in pairs(devIDs) do

    local v = redis.call("HGETALL","device:" .. id)

    v = convertToTable(v)
  
      if v.AgentID then
        if not t_agentNames[v.AgentID] then
          t_agentNames[v.AgentID] = redis.call("HGET","device:"..v.AgentID,"Name")
        end
        
        if t_agentNames[v.AgentID] then
           v.AgentName = t_agentNames[v.AgentID];
        end
        
      end
  
      if v.DomainID then
        if not t_domainNames[v.DomainID] then
          t_domainNames[v.DomainID] = redis.call("HGET","domain:"..v.DomainID,"Name")
        end
        
        if t_domainNames[v.DomainID] then
           v.DomainName = t_domainNames[v.DomainID];
        end
        
      end  
  
      if v.DauID then
        if not t_dauNames[v.DauID] then
          t_dauNames[v.DauID] = redis.call("HGET","dau:"..v.DauID,"Name")
        end
        
        if t_dauNames[v.DauID] then
           v.DauName = t_dauNames[v.DauID];
        end
        
        local daudata = redis.call("HMGET","dau:".. v.DauID,"OnlineStatus","ActualIP","NetPort")
        if "1" == daudata[1] then
          v.DauIP = daudata[2]
          v.DauNetPort = daudata[3]
        end
        
      end  
     
    if v.ActualIP == "None" then
        v.ActualIP = nil;
    end
     
      result[#result+1] = v
  end

  -- rlog("result count : " .. #result)
  local result_json = "[]"
  if #result > 0 then
    result_json = cjson.encode(result)
  end
  

  -- rlog(result_json)

  return  {#result,devIDs,result_json}

end

local function autoCheckDevice(pos,currTime)
   local nextPos = pos +100
   local total = redis.call("ZCARD","Zdevice")
   local groupDevIDs = redis.call("ZRANGE","Zdevice",pos,nextPos)
   local expiredDevs = {}
   local t_dauIps = {}
   local devExtInfoArr = {}
   
   for i,v in pairs(groupDevIDs) do
      local t = {}
   
      local devInfo = redis.call("HMGET","device:"..v,"OnlineStatus","ExpireDate","DauID","MainTypeID","AgentID","Version","HardVersion","RegTime")
    local devExtInfo = {}
    devExtInfo.ID = v
    devExtInfo.Version = devInfo[6] or ""
    devExtInfo.HardVersion = devInfo[7] or ""
    
    if "None" == devExtInfo.Version then
        devExtInfo.Version = ""
    end
    
    if "None" == devExtInfo.HardVersion then
        devExtInfo.HardVersion = ""
    end
    
    
    devExtInfo.VerNtfDate =  tonumber(devInfo[8]) or currTime
    
      if "1" == devInfo[1] then
      devExtInfo.OnlineStatus = 1
          if devInfo[2] and currTime + 0 >= devInfo[2] + 0 then
             t.ID = v
             t.MainTypeID = devInfo[4]
             
             local agentID = devInfo[5]
--             if 5 == tonumber(t.MainTypeID) then
--                agentID = "noagent"
--             end
             local dauID = devInfo[3]
             if dauID then
                deviceOffline("nms_id",dauID,v) -- 下线之
                local dauInfo = redis.call("HMGET","dau:".. dauID,"ActualIP","NetAddress","NetPort")
        
                if not t_dauIps[dauID] then
                  if dauInfo[0] and dauInfo[0] ~= "None" then
                      t_dauIps[dauID] = dauInfo[0]
                  else 
                      t_dauIps[dauID] = dauInfo[1]
                  end
                end
                
                if t_dauIps[dauID] then
                   t.DauIP = t_dauIps[dauID];
                end
                
                t.DauPort = dauInfo[2]
             end
             
             expiredDevs[#expiredDevs+1] = t
             
          end
    else
    devExtInfo.OnlineStatus = 0
      end  
      
  devExtInfoArr[#devExtInfoArr+1] = devExtInfo
   end

  local expiredDevsJson = "[]"
    if #expiredDevs > 0 then
      expiredDevsJson = cjson.encode(expiredDevs)
    end

  local onlineStatJson = "[]"
    if #devExtInfoArr > 0 then
      onlineStatJson = cjson.encode(devExtInfoArr)
    end
  
   -- rlog(expiredDevsJson)
   -- rlog(onlineStatJson)
   return {total,nextPos,#expiredDevs,expiredDevsJson,onlineStatJson}
   
end




local function autoCheckDau(pos,currUptime)
   local nextPos = pos +1
   local total = redis.call("SCARD","dauset")
   local groupDevIDs = redis.call("SMEMBERS","dauset")

   local expiredDevs = {}
   for i=pos,math.min(nextPos,#groupDevIDs) do
      local dauID = groupDevIDs[i]

      local dauInfo = redis.call("HGETALL","dau:"..dauID)
      if next(dauInfo) ~= nil then
          dauInfo = convertToTable(dauInfo)
      end
      
      if dauInfo.HeartBeat and math.abs(currUptime-dauInfo.HeartBeat) >= 60 then
          dauOffline(dauID)
          expiredDevs[#expiredDevs+1] = dauInfo
      end  
      

   end

  local expiredDevsJson = "[]"
    if #expiredDevs > 0 then
      expiredDevsJson = cjson.encode(expiredDevs)
    end

   -- rlog(expiredDevsJson)
   return {total,nextPos,#expiredDevs,expiredDevsJson}
   
end



local function deleteDevice(...)
  redis.replicate_commands()
local devIDs = {}
for i = 1,arg.n do
   devIDs[#devIDs+1] = arg[i]
end


      local all_keys = {};
      local keys = {};
      local cursor = "0"
      repeat
      local result = redis.call("SCAN", cursor, "match", "Sroledevice:*", "count", 10000)
      cursor = result[1];
      keys = result[2];
      for i, key in ipairs(keys) do
        all_keys[#all_keys+1] = key;
      end
      until cursor == "0"
      
      for i, key in ipairs(all_keys) do
          deleteMegaFromSet(devIDs,key)
      end
      
      deleteMegaFromSet(devIDs,"Zdevice")
	  deleteMegaFromSet(devIDs,"onlineDevice")
      deleteMegaFromSet(devIDs,"SnmpDevice")
	  deleteMegaFromSet(devIDs,"KelaoanboxDevice")

for i,v  in pairs(devIDs) do

    
--      local device = redis.call("HGETALL","device:" .. v)

        local devInfo = redis.call("HMGET","device:"..v,"group","DauID","AgentID")
        
      if devInfo then
          local group = devInfo[1]
        local dauID = devInfo[2]
        local agentID = devInfo[3]
      
          if group then
            redis.call("SREM","Sgroupdevice:" .. group,v)
          end
          if dauID then
            redis.call("SREM","daudevice:" .. dauID,v)
          end
          
           if agentID then
          redis.call("SREM","agentdevice:" .. agentID,v)
          redis.call("SREM","agentOnlineDevice:" .. agentID,v)
          end
      end
    
      redis.call("DEL","device:" .. v)

end

    return {"OK"}

end


local function deleteMegaDevice(ta,step)
  local step = step or 1000
  -- 不强制删除
  -- redis.call("DEL",dst)

  for i=1,#ta,step do
    local packTail = math.min(i+step-1,#ta)
    local packSzie = packTail - i + 1
    deleteDevice(unpack(ta,i,packTail))
    
  end

end


local function deleteAgent(agentID)
    redis.replicate_commands()
	cleanupUserTempData()
    
    local agentdev = redis.call("SMEMBERS","agentdevice:" .. agentID)
    redis.call("DEL","agentdevice:" .. agentID)
    redis.call("DEL","agentkdmno:" .. agentID)
    redis.call("DEL","agentOnlineDevice:" .. agentID)
    
    deleteMegaDevice(agentdev)

    return {"OK"}
    
end



local function deleteSnmp(...)
local snmpid = {}
for i = 1,arg.n do
   snmpid[#snmpid+1] = arg[i]
end

cleanupUserTempData()
    
    
    deleteMegaDevice(snmpid)

    return {"OK"}
    
end



local function deleteIntelligentBox(...)
local intelligentBoxid = {}
for i = 1,arg.n do
   intelligentBoxid[#intelligentBoxid+1] = arg[i]
end

	cleanupUserTempData()
    
    
    deleteMegaDevice(intelligentBoxid)

    return {"OK"}
    
end






local function getSyncMark()

    if redis.call("EXISTS","SyncAgentDevice") > 0 then
          return {"SYNC"}
    end

    return {"NOSYNC"}
    
end


local function getDeviceCount()
  local total = redis.call("ZCARD","Zdevice")
  return {total}
end


local function setFlagByName(flagName,exTime)
  redis.call("SET", "Flag-" .. flagName ,"1","EX",exTime);
  return {"OK"}
end

local function delFlagByName(flagName)
  redis.call("DEL", "Flag-" .. flagName);
  return {"OK"}
end

local function chkFlagByName(flagName)

    if redis.call("EXISTS","Flag-" .. flagName) > 0 then
          return {"EXISTS"}
    end

    return {"NOTEXISTS"}
    
end

local function resetSyncDevMark()
  redis.call("SET", "SyncAgentDevice","1","EX","30");
  return {"OK"}
end

local function putEvent(event)

    local lastEventID = tonumber(redis.call("GET","LastEventID"))
    lastEventID = lastEventID or 0
    
    lastEventID = lastEventID + 1
    local e = cjson.decode(event)
    if e.targetSessionId and 0 == #e.targetSessionId then
        e.targetSessionId = nil
    end 
    e.eventId = lastEventID;
    event = cjson.encode(e)
    
    redis.call("SET","LastEventID",lastEventID)
    redis.call("SET","Event-"..lastEventID,event,"EX",300)
    redis.call("PUBLISH","newevent"..lastEventID,"newevent")
    
    return {lastEventID,event}
    
end

local function getEvent(startID,endID)

    local lastEventID = tonumber(redis.call("GET","LastEventID"))
    lastEventID = lastEventID or 0
    endID = endID or lastEventID
    
    local events = {}
    
    for i=startID,endID
    do
        local e = redis.call("GET","Event-"..i)
        if e then
          e = cjson.decode(e)
          events[#events+1] = e
        end
    end
    
   local eventJson = "[]"
  if #events > 0 then
    eventJson = cjson.encode(events)
    end
    
    return {lastEventID,eventJson}
    
end




local switch = {
  getUserGroup = getUserGroup,
  getUserDevice = getUserDevice,
  searchGroupByName = searchGroupByName,
  searchGroupByID = searchGroupByID,
  deviceRegister = deviceRegister,
  dauRegister = dauRegister,
  dauOffline = dauOffline,
  getDau = getDau,
  getAllDau = getAllDau,
  getDauInfoByIDs = getDauInfoByIDs,
  freshDauHeartBeat = freshDauHeartBeat,
  deviceOffline = deviceOffline,
  getDeviceByIDs = getDeviceByIDs,
  getDevUpdateInfoByIDs = getDevUpdateInfoByIDs,
  getAgentStatus = getAgentStatus,
  countOnlineByGroup = countOnlineByGroup,
  autoCheckDevice = autoCheckDevice,
  autoCheckDau = autoCheckDau,
  deleteAgent = deleteAgent,
  deleteSnmp = deleteSnmp,  
  deleteIntelligentBox = deleteIntelligentBox,
  deleteDevice = deleteDevice,
  setAlarmStat = setAlarmStat,
  getNMSIDByKDMNO = getNMSIDByKDMNO,
  getSyncMark = getSyncMark,
  getDeviceCount = getDeviceCount,
  setFlagByName = setFlagByName,
  delFlagByName = delFlagByName,
  chkFlagByName = chkFlagByName,
  resetSyncDevMark = resetSyncDevMark,
  putEvent = putEvent,
  getEvent = getEvent,
  updateDevUniqProp = updateDevUniqProp,
  
  niltail = nil
}


local cmd = switch[KEYS[1]]
if(cmd) then
  return cmd(unpack(KEYS,2))
else
  return "no such method"
end
    
    
    
    
    --    -- rlog(result)
    --   return result
    
    -- ./redis-cli -a password --eval test.lua 
    -- redis-cli --eval ratelimiting.lua rate.limitingl:127.0.0.1 , 10 3
    -- ./redis-cli -a password  SCRIPT LOAD "$(cat test.lua)"
