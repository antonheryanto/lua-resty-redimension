local resty_redis = require 'resty.redis'
local index = require 'resty.redimension'
local concat = table.concat
local key = 'people-by-salary'
local hashkey = 'people-by-salary-map'
local redis = resty_redis:new()
redis:set_timeout(1000)
local ok, err = redis:connect('127.0.0.1', 6379)
if not ok then print('error:', err) end

redis:del(key)
redis:del(hashkey)
local idx = index:new(redis, key, 2)
idx.hashkey = hashkey
idx:index({45, 120000}, 'Josh')
idx:index({50, 110000}, 'Pamela')
idx:index({41, 100000}, 'George')
idx:index({30, 125000}, 'Angela')

local function debug()
    local result = idx:query({{40, 50}, {100000, 115000}})
    for i = 1, #result do
        print(concat(result[i], ' -> '))
    end
end

print "second test"
debug()
idx:unindex_by_id("Pamela")
print "print after unindex:"
debug()
idx:update({42, 100000}, "George")
print "After updating:"
debug()
