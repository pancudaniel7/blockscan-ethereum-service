#!lua name=blockchain
local function upk(t, i, j)
	i = i or 1; j = j or #t
	if i > j then return end
	return t[i], upk(t, i + 1, j)
end

redis.register_function{
	function_name='add_block',
	callback=function(keys, args)
		local ttl = tonumber(args[1])
		local id  = args[2]
		if not ttl or not id then return {0, false} end
		if redis.call('SET', keys[1], '1', 'NX', 'PX', ttl) then
			local msg_id = redis.call('XADD', keys[2], id, upk(args, 3))
			return {1, msg_id}
		end
		return {0, false}
	end
}
