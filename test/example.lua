local resty_redis = require 'resty.redis'
local index = require 'resty.redimension'
local concat = table.concat

local redis = resty_redis:new()
redis:set_timeout(1000)
local ok, err = redis:connect('127.0.0.1', 6379)
if not ok then print('error:', err) end

local idx = index:new(redis, 'people-by-salary', 2)
idx:index({45, 120000}, 'Josh')
idx:index({50, 110000}, 'Pamela')
idx:index({30, 125000}, 'Angela')

print "first test"
local result = idx:query({{45, 50}, {100000, 115000}})
for i = 1, #result do
    print(concat(result[i],' -> '))
end
