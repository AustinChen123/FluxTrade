-- update_position.lua
-- Keys: 
--  1. state:balance:{account_id} (Hash)
--  2. state:position:{strategy_id}:{product_id} (Hash)
--  3. stream:trades (Stream)

-- Args:
--  1. account_id
--  2. strategy_id
--  3. product_id
--  4. side (BUY/SELL)
--  5. quantity
--  6. price
--  7. timestamp
--  8. trade_id
--  9. order_id

local account_id = ARGV[1]
local strategy_id = ARGV[2]
local product_id = ARGV[3]
local side = ARGV[4]
local quantity = tonumber(ARGV[5])
local price = tonumber(ARGV[6])
local timestamp = ARGV[7]
local trade_id = ARGV[8]
local order_id = ARGV[9]

local cost = quantity * price
local balance_key = "state:balance:" .. account_id
local position_key = "state:position:" .. strategy_id .. ":" .. product_id

-- 1. Check Balance (only for BUY)
if side == "BUY" then
    local current_balance = tonumber(redis.call("HGET", balance_key, "free") or "0")
    if current_balance < cost then
        return redis.error_reply("INSUFFICIENT_BALANCE: Req " .. cost .. ", Avail " .. current_balance)
    end
    -- Deduct Cost
    redis.call("HINCRBYFLOAT", balance_key, "free", -cost)
    redis.call("HINCRBYFLOAT", balance_key, "used", cost) -- Assuming locked for position
end

-- 2. Update Position
local current_pos_qty = tonumber(redis.call("HGET", position_key, "quantity") or "0")
local new_pos_qty = 0

if side == "BUY" then
    new_pos_qty = current_pos_qty + quantity
else
    new_pos_qty = current_pos_qty - quantity
    -- Release cost if closing (simplified) - Real logic needs avg entry price, etc.
    -- For now, we assume simple spot logic or let a separate reconciliation handle PnL
    local release_amt = quantity * price 
    redis.call("HINCRBYFLOAT", balance_key, "free", release_amt)
    redis.call("HINCRBYFLOAT", balance_key, "used", -release_amt) -- Release lock
end

redis.call("HSET", position_key, "quantity", new_pos_qty)
redis.call("HSET", position_key, "last_update", timestamp)

-- 3. XADD to stream:trades
redis.call("XADD", "stream:trades", "*", 
    "trade_id", trade_id,
    "order_id", order_id,
    "strategy_id", strategy_id,
    "product_id", product_id,
    "side", side,
    "price", price,
    "quantity", quantity,
    "timestamp", timestamp
)

return "OK"
