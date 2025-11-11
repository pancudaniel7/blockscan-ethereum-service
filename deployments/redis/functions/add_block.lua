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

        -- Acquire dedup key first; if it exists, drop this invocation
        if not redis.call('SET', keys[1], '1', 'NX', 'PX', ttl) then
            -- Dedup already exists
            return {0, 'EXISTS'}
        end

        -- Append to stream; on failure, revert dedup key
        local ok, msg_id = pcall(redis.call, 'XADD', keys[2], id, upk(args, 3))
        if not ok or not msg_id then
            redis.call('DEL', keys[1])
            return {0, 'XADD_ERR'}
        end
        return {1, msg_id}
    end
}
