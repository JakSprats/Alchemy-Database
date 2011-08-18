
local AllSynced = false;
function RsubscribeAlchemySync() --print ('RsubscribeAlchemySync');
  local nsync = 0;
  for num, data in pairs(NodeData) do
    if (num ~= MyNodeId and data['synced'] == 0) then
      print ('RSUBSCRIBE ip: ' .. data['ip'] .. ' port: ' .. data['port']);
      local ret = redis("RSUBSCRIBE", data['ip'], data['port'], 'sync');
      if (ret["err"] == nil) then
        data['synced'] = 1; 
        nsync = nsync + 1;
        print ('SYNCED ip: ' .. data['ip'] .. ' port: ' .. data['port']);
      else
        data['synced'] = 0;
      end
    else
      nsync = nsync + 1;
    end
  end
  if (nsync == NumNodes) then
    if (AllSynced == false) then
      AllSynced = true;
      print ('AllSynced');
    end
    return true;
  end
  return false;
end

-- defines SimulateNetworkPartition
--dofile "./docroot/DIST/debug.lua";

