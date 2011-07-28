
-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
local SimulateNetworkPartition = 10;

-- ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC
local AllSynced = false;
function RsubscribeAlchemySync() -- lua_cron function, called every second
  --print ('DEBUG: RsubscribeAlchemySync');
  local nsync = 0;
  for num,data in pairs(NodeData) do
    if (num ~= MyNodeId and data['synced'] == 0) then
      local continue = false; -- LUA does not have "continue"
      if (SimulateNetworkPartition ~= 0) then
        if (MyNodeId < 3 and num >= 3) then continue = true; end -- [1<->2]
        if (MyNodeId >= 3 and num < 3) then continue = true; end -- [3<->4]
      end
      if (continue == false) then
        print ('RSUBSCRIBE ip: ' .. data['ip'] .. ' port: ' .. data['port']);
        local ret = redis("RSUBSCRIBE", data['ip'], data['port'], 'sync');
        if (ret["err"] == nil) then
          data['synced'] = 1; 
          nsync = nsync + 1;
          print ('SYNCED ip: ' .. data['ip'] .. ' port: ' .. data['port']);
        else
          data['synced'] = 0;
        end
      end
    else
      nsync = nsync + 1;
    end
  end
  if (SimulateNetworkPartition ~= 0) then
    SimulateNetworkPartition = SimulateNetworkPartition - 1;
    print ('SimulateNetworkPartition: ' .. SimulateNetworkPartition);
    return false;
  end
  if (nsync == NumNodes) then
    if (AllSynced == false) then
      AllSynced = true;
      print ('AllSynced');
    end
    return true;
  end
  print ('nsync: ' .. nsync .. ' NumNodes: ' .. NumNodes);
  return false;
end
