local new_tab = require 'table.new'
local bit = require 'bit'
local setmetatable = setmetatable
local assert = assert
local tonumber = tonumber
local concat = table.concat
local rep = string.rep
local sub = string.sub
local find = string.find
local lower = string.lower
local floor = math.floor
local BIN2HEX = {
    ['0000'] = '0',
    ['0001'] = '1',
    ['0010'] = '2',
    ['0011'] = '3',
    ['0100'] = '4',
    ['0101'] = '5',
    ['0110'] = '6',
    ['0111'] = '7',
    ['1000'] = '8',
    ['1001'] = '9',
    ['1010'] = 'A',
    ['1011'] = 'B',
    ['1100'] = 'C',
    ['1101'] = 'D',
    ['1110'] = 'E',
    ['1111'] = 'F'
}
local MAX_PRECISSION = 56
local ERR_DIM = 'wrong number of values for this index'
local ERR_HASHKEY = 'Please specifiy an hash key to enable mapping'
local _M = {}
local mt = { __index = _M }
_M.VERSION = '0.1.0'

-- JIT string split
function _M.split(self, delimiter)
    local result = new_tab(#self, 0)
    local index = 0
    local n = 1
    while true do
        local pos = find(self, delimiter, index, true)
        if not pos then
            result[n] = sub(self, index) -- Save what's left
            break
        end

        result[n] = sub(self,index, pos - 1) -- Save it in our array.
        index = pos + 1
        n = n + 1
    end

    return result, n
end

-- Encode N variables into the bits-interleaved representation.
function _M.encode(self, args)
    local dim = self.dim
    local prec = self.prec
    local n = #args
    local comb = new_tab(prec, 1)
    for i = 1, n do
        local b = args[i]
        for j = 1, prec do
            b = bit.rol(b, 1)
            comb[j] = comb[j] and comb[j] .. bit.band(b, 1) or bit.band(b, 1)
        end
    end

    local bs = concat(comb)
    local l = #bs
    local rem = l % 4
    if (rem > 0) then
        bs = rep('0', 4 - rem) .. bs
    end

    local as = new_tab(l/4, 0)
    local j = 0
    for i = 1, l - 1, 4 do
        j = j + 1
        as[j] = BIN2HEX[sub(bs, i, i+3)]
    end

    local hs = concat(as)
    return rep('0', prec * dim / 4 - #hs) .. lower(sub(hs, 3))
end

-- Encode an element coordinates and ID as the whole string to add
-- into the sorted set.
function _M.elestring(self, vars, id)
    local dim = self.dim
    local n = #vars
    assert(n == dim, ERR_DIM)
    local ele = new_tab(n + 1, 0)
    ele[1] = _M.encode(self, vars)
    for i=1, n do
        ele[i + 1] = vars[i]
    end
    ele[n+2] = id
    return concat(ele, ':')
end

--- exp is the exponent of two that gives the size of the squares
---- we use in the range query. N times the exponent is the number
---- of bits we unset and set to get the start and end points of the range.
function _M.query_raw(self, vrange, exp)
    local redis = self.redis
    local key = self.key
    local dim = self.dim
    local n = #vrange
    local vstart = new_tab(n, 0)
    local vend = new_tab(n, 0)
    -- We start scaling our indexes in order to iterate all areas, so
    -- that to move between N-dimensional areas we can just increment
    -- vars.
    for i=1, n do
        vstart[i] = floor(vrange[i][1]/(2^exp))
        vend[i] = floor(vrange[i][2]/(2^exp))
    end

    -- Visit all the sub-areas to cover our N-dim search region.
    local vcurrent = new_tab(n, 0)
    for i = 1, n do
        vcurrent[i] = vstart[i]
    end

    local ranges = {}
    local ri = 0
    local notdone = true
    while notdone do
        -- For each sub-region, encode all the start-end ranges
        -- for each dimension.
        local vrange_start = new_tab(dim, 0)
        local vrange_end = new_tab(dim, 0)
        for i = 1, dim do
            vrange_start[i] = vcurrent[i]*(2^exp)
            vrange_end[i] = bit.bor(vrange_start[i],(2^exp)-1)
        end

        -- Now we need to combine the ranges for each dimension
        -- into a single lexicographcial query, so we turn
        -- the ranges it into interleaved form.
        local s = _M.encode(self, vrange_start)
        -- Now that we have the start of the range, calculate the end
        -- by replacing the specified number of bits from 0 to 1.
        local e = _M.encode(self, vrange_end)
        ri = ri + 1
        ranges[ri] = {'['..s, '['..e..':\255'}

        -- Increment to loop in N dimensions in order to visit
        -- all the sub-areas representing the N dimensional area to
        -- query.
        for i = 1, dim do
            if vcurrent[i] ~= vend[i] then
                vcurrent[i] = vcurrent[i] + 1
                break
            elseif i == dim then
                notdone = false -- Visited everything!
            else
                vcurrent[i] = vstart[i]
            end
        end
    end

    -- Perform the ZRANGEBYLEX queries to collect the results from the
    -- defined ranges. Use pipelining to speedup.
    local allres = {}
    redis:init_pipeline()
    for i=1, #ranges do
        redis:zrangebylex(key, ranges[i][1], ranges[i][2])
    end
    local res = redis:commit_pipeline()
    local k = 0
    for i=1, #res do
        for j=1, #res[i] do
            k = k + 1
            allres[k] = res[i][j]
        end
    end

    -- Filter items according to the requested limits. This is needed
    -- since our sub-areas used to cover the whole search area are not
    -- perfectly aligned with boundaries, so we also retrieve elements
    -- outside the searched ranges.
    local items = {}
    local ii = 0
    for j = 1, #allres do
        local v = allres[j]
        local fields = _M.split(v,':')
        local skip = false
        for i = 1, dim do
            if tonumber(fields[i+1]) < vrange[i][1]
            or tonumber(fields[i+1]) > vrange[i][2]
            then
                skip = true
                break
            end
        end
        if not skip then
            ii = ii + 1
            items[ii] = new_tab(#fields - 1, 0)
            for i = 2, #fields do
                items[ii][i - 1] = fields[i]
            end
        end
    end

    return items
end

function _M.new(self, redis, key, dim, prec)
    redis.add_commands('zrangebylex', 'zrevrangebylex', 'zremrangebylex')
    return setmetatable({
        prec = prec or MAX_PRECISSION,
        dim = dim,
        key = key,
        hashkey = false,
        redis = redis
    }, mt)
end

function _M.index(self, vars, id)
    local redis = self.redis
    local key = self.key
    local ele = _M.elestring(self, vars, id)
    local hashkey = self.hashkey
    redis:zadd(key, 0, ele)
    if hashkey then
        redis:hset(hashkey, id, ele)
    end
end

-- ZREM according to current position in the space and ID.
function _M.unindex(self, vars, id)
    local redis = self.redis
    local key = self.key
    return redis:zrem(key, _M.elestring(self, vars,id))
end

-- Unidex by just ID in case @hashkey is set to true in order to take
-- an associated Redis hash with ID -> current indexed representation,
-- so that the user can unindex easily.
function _M.unindex_by_id(self, id)
    local hashkey = assert(self.hashkey, ERR_HASHKEY)
    local redis = self.redis
    local key = self.key
    local ele, err = redis:hget(hashkey, id)
    if not ele then
        error(err)
    end

    redis:zrem(key, ele)
    redis:hdel(hashkey, id)
end

-- Like index but makes sure to remove the old index for the specified
-- id. Requires hash mapping enabled.
function _M.update(self, vars, id)
    local hashkey = assert(self.hashkey, ERR_HASHKEY)
    local redis = self.redis
    local key = self.key
    local ele = _M.elestring(self, vars, id)
    local oldele = redis:hget(hashkey, id)
    redis:zrem(key, oldele)
    redis:hdel(hashkey, id)
    redis:zadd(key, 0, ele)
    redis:hset(hashkey, id, ele)
end

-- Like query_raw, but before performing the query makes sure to order
-- -- parameters so that x0 < x1 and y0 < y1 and so forth.
-- -- Also calculates the exponent for the query_raw masking.
function _M.query(self, vrange)
    local dim = self.dim
    local n = #vrange
    assert(n == dim, ERR_DIM)

    local deltas = new_tab(n, 0)
    for i = 1, n do
        if vrange[i][1] > vrange[i][2] then
            vrange[i][1], vrange[i][2] = vrange[i][2], vrange[i][1]
        end
        deltas[i] = vrange[i][2]-vrange[i][1]+1
    end

    local delta = deltas[1]
    for i = 1, n do
        if deltas[i] < delta then
            delta = deltas[i]
        end
    end

    local exp = 1
    while delta > 2 do
        delta = floor(delta / 2)
        exp = exp + 1
    end

    -- If ranges for different dimensions are extremely different in span,
    -- we may end with a too small exponent which will result in a very
    -- big number of queries in order to be very selective. This is most
    -- of the times not a good idea, so at the cost of querying larger
    -- areas and filtering more, we scale 'exp' until we can serve this
    -- request with less than 20 ZRANGEBYLEX commands.
    --
    -- Note: the magic "20" depends on the number of items inside the
    -- requested range, since it's a tradeoff with filtering items outside
    -- the searched area. It is possible to improve the algorithm by using
    -- ZLEXCOUNT to get the number of items.
    while true do
        for i = 1, n do
            deltas[i] = (vrange[i][2]/(2^exp))-(vrange[i][1]/(2^exp))+1
        end
        local ranges = 1
        for i = 1, n do
            ranges = ranges * deltas[i]
        end

        if ranges < 20 then
            break
        end
        exp = exp + 1
    end

    return _M.query_raw(self, vrange, exp)
end

return _M
