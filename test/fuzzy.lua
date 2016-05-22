local new_tab = require 'table.new'
local resty_redis = require 'resty.redis'
local index = require 'resty.redimension'
local randomseed = math.randomseed
local random = math.random
local now = ngx.now
local KEY = 'redim-fuzzy'
local TOTAL = 1000

local function fuzzy_test(dim, items, queries)
    local redis = resty_redis:new()
    redis:set_timeout(1000)
    local ok, err = redis:connect('127.0.0.1', 6379)
    if not ok then print('error:', err) end

    redis:del(KEY)
    local idx = index:new(redis, KEY, dim)
    local id = 0
    local dataset = new_tab(TOTAL, 0)
    randomseed(now())
    for i = 1, TOTAL do
        local vars = new_tab(dim, 0)
        for j = 1, dim do
            vars[j] = random(1000)
        end
        idx:index(vars, id)
        vars[dim + 1] = id
        dataset[i] = vars
        id = id + 1
    end

    -- table sorting is so much FUN!
    local function cmp(a, b, depth)
        depth = depth or 1

        if depth > dim + 1 then
            return false
        end
        if a[depth] < b[depth] then
            return true
        end
        if a[depth] == b[depth] then
            return cmp(a, b, depth + 1)
        end
        if a[depth] > b[depth] then
            return false
        end
    end

    for i = 1, TOTAL do
        local randoms = new_tab(dim, 0)
        for j = 1, dim do
            local s = random(1000)
            local e = random(1000)
            if e < s then
                s, e = e, s
            end
            randoms[j] = {s, e}
        end
        -- local st = now()
        local r1 = idx:query(randoms)
        for k = 1, #r1 do
            for j = 1, #r1[k] do
                r1[k][j] = tonumber(r1[k][j])
            end
        end

        -- local et = now()
        -- print (#r1, ' result in ', et - st, ' seconds')

        local n2 = 1
        local r2 = new_tab(TOTAL, 0)
        for k = 1, TOTAL do
            local included = true
            for j = 1, dim do
                if dataset[k][j] < randoms[j][1]
                or dataset[k][j] > randoms[j][2] then
                    included = false
                end
            end
            if included then
                r2[n2] = dataset[k]
                n2 = n2 + 1
            end
        end
        assert(#r1 == #r2, 'result not similar')
        table.sort(r1, cmp)
        table.sort(r2, cmp)
        for k = 1, #r1 do
            for j = 1, #r1[k] do
                if r1[k][j] ~= r2[k][j] then
                    error('ERROR ', j, ':', r1[k][j], ' ~= ', r2[k][j])
                end
            end
        end
    end
    print(dim,'D test passed')
    redis:del(KEY)
end

print "third test (fuzzy)"
fuzzy_test(4,100,1000)
fuzzy_test(3,100,1000)
fuzzy_test(2,1000,1000)
