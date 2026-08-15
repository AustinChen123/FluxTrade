-- update_position.lua
-- Keys: none. The caller supplies a fully calculated position projection.

-- Args:
--  1. strategy_id
--  2. product_id
--  3. side (BUY/SELL)
--  4. quantity
--  5. price
--  6. timestamp
--  7. trade_id
--  8. order_id
--  9. projected signed position quantity
-- 10. projected entry price

local strategy_id = ARGV[1]
local product_id = ARGV[2]
local side = ARGV[3]
local quantity = ARGV[4]
local price = ARGV[5]
local timestamp = ARGV[6]
local trade_id = ARGV[7]
local order_id = ARGV[8]
local position_quantity = ARGV[9]
local entry_price = ARGV[10]
local position_key = "state:position:" .. strategy_id .. ":" .. product_id

redis.call("HSET", position_key, "quantity", position_quantity)
redis.call("HSET", position_key, "entry_price", entry_price)
redis.call("HSET", position_key, "last_update", timestamp)

-- Preserve the existing stream:trades field set.
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
