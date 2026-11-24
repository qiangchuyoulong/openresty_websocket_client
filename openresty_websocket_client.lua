
local client    = require "resty.websocket.client"
local cjson     = require "cjson.safe"
local semaphore = require "ngx.semaphore"
local nodeID    = 134

local _M = {}

local UPSTREAMS = {
    chat = { host = "ws://192.168.228.134", port = 9001, path = "/intersub" },
    game = { host = "ws://192.168.228.134", port = 9002, path = "/intersub" },
}

-- ===== 心跳参数 =====
-- RECV_TIMEOUT 只作用于单次 recv_frame() 调用，超时返回不判死，
-- 仅用于让 reader_loop 周期性醒来做心跳/判死检查。
local RECV_TIMEOUT  = 5       -- recv_frame 单次阻塞时长（秒），轮询粒度
local PING_INTERVAL = 20      -- 主动向上游发 ping 的间隔（秒）
local DEAD_TIMEOUT  = 90      -- 超过这个时间没收到任何帧（含 pong）→ 判死

local conns     = {}    -- name -> ws client 对象
local send_q    = {}    -- name -> { {kind=..., data=..., reqid=...}, ... }
local send_sema = {}    -- name -> semaphore
local pending   = {}    -- reqid -> { resp={}, done=false, sema=semaphore, upstream=name }
local req_seq   = 0

local function gen_reqid()
    req_seq = req_seq + 1
    return string.format("%d_%d_%d_%d",
        nodeID, ngx.worker.pid(),
        math.floor(ngx.now() * 1000),   -- 毫秒时间戳
        req_seq)
end

-- ===== init_worker 上下文的线程 =====

-- 收上游的包，写进 pending，通知等待中的浏览器请求；同时做心跳
local function reader_loop(name, cfg)
    local MAX_BACKOFF = 60
    local backoff     = 0
    local last_rx     = ngx.now()   -- 最后一次收到任何帧（含 pong）的时间
    local last_ping   = ngx.now()   -- 最后一次主动 ping 的时间

    local function fail_all_pending(reason)
        for reqid, p in pairs(pending) do
            if p.upstream == name then
                table.insert(p.resp, cjson.encode({
                    error = reason, reqid = reqid, done = true,
                }))
                p.done = true
                p.sema:post()
                pending[reqid] = nil
            end
        end
    end

    local function do_connect()
        local uc, nerr = client:new({ timeout = RECV_TIMEOUT * 1000 })
        if not uc then return nil, "client:new: " .. tostring(nerr) end
        local ok, cerr = uc:connect(cfg.host .. ":" .. cfg.port .. cfg.path)
        if not ok then return nil, "connect: " .. tostring(cerr) end
        return uc
    end

    while not ngx.worker.exiting() do
        local c = conns[name]
        local need_reconnect = false

        -- Worker 退出时直接放弃重连逻辑，不再休眠阻塞
        if ngx.worker.exiting() then
            break
        end

        if c then
            local data, typ, err = c:recv_frame()
            local now = ngx.now()

            if c.fatal then
                ngx.log(ngx.ERR, "upstream ", name, " fatal: ", err)
                conns[name] = nil
                fail_all_pending("upstream fatal")
                need_reconnect = true

            elseif data then
                last_rx = now                    -- 任何帧都刷新活性

                if typ == "ping" then
                    -- 上游主动 ping（少见），回显 payload
                    local ok, perr = pcall(c.send_pong, c, data)
                    if not ok then
                        ngx.log(ngx.ERR, "upstream ", name, " send_pong fail: ", perr)
                    end

                elseif typ == "pong" then
                    -- 心跳应答，last_rx 已刷新，什么都不做

                elseif typ == "close" then
                    pcall(c.send_close, c, 1000)
                    conns[name] = nil
                    fail_all_pending("upstream closed")
                    need_reconnect = true

                else
                    -- text / binary / continuation 业务帧
                    ngx.log(ngx.DEBUG, "reader_loop data: ", data)
                    local ok, msg = pcall(cjson.decode, data)
                    if not ok or type(msg) ~= "table" then
                        ngx.log(ngx.ERR, "reader_loop bad payload: ", data)
                    elseif msg.reqid then
                        local p = pending[msg.reqid]
                        if p then
                            table.insert(p.resp, data)
                            p.done = (msg.done == true)
                            p.sema:post()
                            if p.done then pending[msg.reqid] = nil end
                        end
                    end
                end

            elseif err and err:find("timeout", 1, true) then
                -- 短超时轮询：不立即断，检查是否心跳超时
                -- lua-resty-websocket 把 timeout 包装成
                -- "failed to receive the first N bytes: timeout"，用 find 匹配
                if now - last_rx > DEAD_TIMEOUT then
                    ngx.log(ngx.ERR, "upstream ", name,
                            " heartbeat timeout (", now - last_rx,
                            "s no frame), reconnecting")
                    conns[name] = nil
                    fail_all_pending("upstream heartbeat timeout")
                    need_reconnect = true
                end
                -- 否则只是空闲，静默继续

            else
                ngx.log(ngx.ERR, "upstream ", name,
                        " closed: ", err, ", reconnecting")
                conns[name] = nil
                fail_all_pending("upstream disconnected")
                need_reconnect = true
            end
            -- 心跳：到点就入队 ping（由 sender_loop 串行发送，避免 busy writing）
            -- 注意这是"轮询式心跳"：recv_frame 每 5 秒超时打断一次，
            -- 顺便检查是否到 20 秒该发 ping。抖动上限 = RECV_TIMEOUT。
            if not need_reconnect and conns[name]
               and now - last_ping >= PING_INTERVAL then
                table.insert(send_q[name], { kind = "ping" })
                send_sema[name]:post()
                last_ping = now
                ngx.log(ngx.DEBUG, "upstream ", name, " ping sent")
            end

        else
            need_reconnect = true
        end

        if need_reconnect then
            -- Worker 退出时直接跳过重连休眠，不阻塞退出流程
            if ngx.worker.exiting() then
                ngx.log(ngx.INFO, "upstream ", name, " worker exiting, skip reconnect")
                break
            end
            if backoff > 0 then ngx.sleep(backoff) end

            local uc, cerr = do_connect()
            if uc then
                conns[name] = uc
                backoff    = 0
                last_rx    = ngx.now()   -- 重置活性基准
                last_ping  = ngx.now()
                ngx.log(ngx.INFO, "upstream ", name, " connected")
            else
                backoff = math.min(math.max(backoff * 2, 1), MAX_BACKOFF)
                ngx.log(ngx.ERR, "upstream ", name,
                        " connect failed: ", cerr, ", retry in ", backoff, "s")
            end
        end
    end

    -- 退出时主动关闭 WebSocket 连接，释放底层 socket
    ngx.log(ngx.INFO, "upstream ", name, " reader_loop exiting, closing ws connection")
    local c = conns[name]
    if c then
        -- 主动发送 close 帧，优雅断开上游 WebSocket 连接
        pcall(function() c:send_close(1000, "worker exiting") end)
        -- 强制关闭底层 socket，避免 CLOSE-WAIT 残留
        pcall(function() c:close() end)
        conns[name] = nil
    end
    -- 退出时强制唤醒所有等待中的 pending 请求，防止协程卡死
    fail_all_pending("worker_shutting_down")
end

-- 唯一持有上游写权限的线程：业务帧 + 心跳 ping 都从这里走
local function sender_loop(name)
    while not ngx.worker.exiting() do
        -- 超时时间从 3600s 缩短为 5s，配合轮询检测退出状态，避免长时间卡住
        local ok, err = send_sema[name]:wait(5)
        -- 正常收到信号或超时唤醒，都继续循环检查退出状态
        local q = send_q[name]
        while q[1] do
            local item = table.remove(q, 1)
            local c = conns[name]
            if c then
                local ok, err
                if item.kind == "ping" then
                    ok, err = c:send_ping()
                -- elseif item.kind == "pong" then
                --     ok, err = c:send_pong(item.payload)
                elseif item.kind == "close" then
                    ok, err = c:send_close(item.code or 1000)
                else
                    ngx.log(ngx.DEBUG, "sender ", name, " text: ", item.data)
                    ok, err = c:send_text(item.data)
                end
                if not ok then
                    ngx.log(ngx.ERR, "sender ", name, " send fail: ", err)
                end
            end
            -- conns[name] 为 nil 时直接丢弃（重连期间的入队消息）
        end
    end

    -- sender_loop 退出收尾：清空发送队列，关闭残留连接
    ngx.log(ngx.INFO, "upstream ", name, " sender_loop exiting")
    send_q[name] = {}
    local c = conns[name]
    if c then
        pcall(function() c:close() end)
        conns[name] = nil
    end
end

function _M.init()
    for name, cfg in pairs(UPSTREAMS) do
        send_q[name]    = {}
        send_sema[name] = semaphore.new()

        local uc, err = client:new({ timeout = RECV_TIMEOUT * 1000 })
        local ok = uc and uc:connect(cfg.host .. ":" .. cfg.port .. cfg.path)
        conns[name] = ok and uc or nil
        if not ok then
            ngx.log(ngx.ERR, "init connect ", name, " failed: ",
                    err or "?", " (reader 会重试)")
        end

        ngx.thread.spawn(reader_loop, name, cfg)
        ngx.thread.spawn(sender_loop, name)
    end

    -- 全局 Watchdog 协程：监控 Worker 退出状态，强制清理所有资源
    ngx.thread.spawn(function()
        while true do
            if ngx.worker.exiting() then
                ngx.log(ngx.INFO, "[GATEWAY] Worker exiting detected, force cleanup all upstream ws")
                -- 遍历所有上游，强制关闭 WebSocket 连接
                for name, _ in pairs(UPSTREAMS) do
                    local c = conns[name]
                    if c then
                        pcall(function() c:send_close(1000, "worker exiting") end)
                        pcall(function() c:close() end)
                        conns[name] = nil
                    end
                    -- 唤醒 sender_loop 立刻退出，无需等待信号量超时
                    send_sema[name]:post()
                end
                -- 通知所有挂起的请求失败，避免协程死锁
                for reqid, p in pairs(pending) do
                    p.done = true
                    p.sema:post()
                    pending[reqid] = nil
                end
                return
            end
            ngx.sleep(0.5)
        end
    end)

    ngx.log(ngx.DEBUG, "PROXY init over")
end

-- ===== content 请求上下文调用 =====

-- 发一个请求并等待完整响应（可多帧），超时返回 nil, "timeout"
-- 参数：name 上游名，msg 业务负载（任意可 JSON 序列化的表），reqid 请求唯一 ID
-- 返回：resp 数组（上游每条消息一元素），或 nil, err
function _M.call(name, msg)
    -- ngx.log(ngx.ERR, "call:---", name, cjson.encode(msg))

    local c = conns[name]
    if not c then
        return nil, "upstream unavailable"
    end
    -- reqid 由模块内部生成，调用方无法指定，天然唯一
    local reqid = gen_reqid()

    local p = { resp = {}, done = false, sema = semaphore.new(), upstream = name }
    pending[reqid] = p

    -- 显式请求体结构：不污染调用方传入的 msg
    table.insert(send_q[name], {
        reqid = reqid,
        data  = cjson.encode({
            reqid       = reqid,
            pid         = ngx.worker.pid(),
            nodeid      = nodeID,
            payload     = msg,
        }),
    })
    send_sema[name]:post()

    local deadline = ngx.now() + 60
    while not p.done do
        local remain = deadline - ngx.now()
        if remain <= 0 then
            pending[reqid] = nil
            return nil, "timeout"
        end
        p.sema:wait(remain)   -- 收到一帧就醒一次
    end

    pending[reqid] = nil
    return p.resp
end

return _M
