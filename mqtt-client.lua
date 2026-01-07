local colors = require("colors")
local component = require("component")
local computer = require("computer")
local event = require "event"
local internet = require("internet")
local json = require("json")
local serialization = require("serialization")

local mqttLib = require("mqtt-lib")

local coroutines = {}

function escape_unprintables(s)
    local escaped = ""
    for i = 1, #s do
        local char_byte = string.byte(s, i)
        if char_byte >= 32 and char_byte <= 126 then
            -- Printable ASCII characters
            if char_byte == string.byte('\\') then
                escaped = escaped .. "\\\\" -- Escape backslashes
            elseif char_byte == string.byte('"') then
                escaped = escaped .. "\\\"" -- Escape double quotes if needed
            else
                escaped = escaped .. string.char(char_byte)
            end
        else
            -- Unprintable or extended characters (use hex escape)
            escaped = escaped .. string.format("\\x%02X", char_byte)
        end
    end
    return escaped
end

function be16(num)
  local L = num % 256
  local H = num >> 8
  return string.char(H, L)
end

function length_prefix_string(str)
  return be16(string.len(str)) .. str
end

function varint(num)
  if num < 128 then
    return string.char(num)
  else
    print("unsupported length", num)
  end
end

function create_packet(code, payload)
  print("packet length", string.len(payload))
  return string.char(code) .. varint(string.len(payload)) .. payload
end

function create_connect_packet()
  local clientid = "opencomputer"
  local user = "oc";
  local password = "hunter2"

  local flags = 0xc2 -- user, password, clean session
  local packet = length_prefix_string("MQTT")
  packet = packet .. string.char(4) -- MQTT v3.1.1
  packet = packet .. string.char(flags)
  packet = packet .. be16(600) -- keep alive
  packet = packet .. length_prefix_string(clientid)
  packet = packet .. length_prefix_string(user) .. length_prefix_string(password)

  return create_packet(1 << 4, packet)
end

function process_one_packet(hnd)
  print("got packet", hnd)
  local header = hnd:read(2)
  print(escape_unprintables(header))
  local len = string.byte(header, 2)
  local payload = hnd:read(len)
  print(escape_unprintables(payload))
end


--print(escape_unprintables(t))

--print(escape_unprintables(varint(46)))

--local handle = internet.open("10.0.0.11", 1883)
--print("opened", handle)

--handle:finishConnect()
--print("finished")

--handle:write(create_connect_packet())
--print("wrote")
--handle:flush()

--event.listen("internet_ready", process_one_packet)
--print("listened")

--for i=0,60,1 do
--  os.sleep(1)
--  print("slept")
--end

--process_one_packet(handle)

local mqtt = mqttLib:new("10.0.0.11", 1883, "oc", "hunter2", computer.address(), 600)

local function get_device(addr)
  local dev = {}
  dev["name"] = "opencomputer redstone " .. addr
  dev["serial_number"] = addr
  dev["identifiers"] = { addr }
  return dev
end

local function register_redstone(addr)
  local dev = component.proxy(addr)
  for i=0,15,1 do
    local config = {}
    local c = colors[i]
    config["device"] = get_device(addr)
    config["command_topic"] = "oc-computer/" .. addr .. "/" .. i .. "/set"
    config["name"] = "east redstone color " .. colors[i]
    config["platform"] = "switch"
    config["unique_id"] = addr .. "/" .. i

    mqtt:publish("homeassistant/switch/" .. addr .. "/" .. i .. "/config", json:encode(config))
  end
end

local function try(callback)
  return function(...)
    local result = table.pack(xpcall(callback, debug.traceback, ...))
    if not result[1] then
      event.push("exception", result[2])
    end
    return table.unpack(result,2)
  end
end

local function loop()
  mqtt:loop()
end

local function main()
  print("start")
  mqtt:registerWillMessage("oc-computer/online", "offline", 0, true)
  mqtt:registerOnConnect(function ()
    print("connected")
  end)
  mqtt:connect()
  mqtt:publish("oc-computer/online", "online")
  mqtt:subscribe("oc-computer/command/+", 0, function (topic, message, topicValues)
    print("topic: " .. topic .. " message: " .. message .. " topicValues: " .. serialization.serialize(topicValues))
  end)

  mqtt:subscribe("oc-computer/+/+/set", 0, function (topic, message, topicValues)
    print("topic: " .. topic .. " message: " .. message .. " topicValues: " .. serialization.serialize(topicValues))
    local addr = topicValues[1]
    local color = tonumber(topicValues[2])
    local dev = component.proxy(addr)
    if message == "ON" then
      dev.setBundledOutput(5, color, 255)
    else
      dev.setBundledOutput(5, color, 0)
    end
  end)

  for k,v in component.list() do
    print(k,v)
    if v == "redstone" then
      register_redstone(k)
    end
  end

  table.insert(coroutines, event.timer(10, try(loop), math.huge))
  local eventName, message = event.pullMultiple("interrupted", "exception")
  for _, coroutine in pairs(coroutines) do
    if type(coroutine) == "table" and coroutine.kill then
      coroutine:kill()
    elseif type(coroutine) == "number" then
      event.cancel(coroutine)
    end
  end

  if eventName == "exception" then
    io.stderr:write(message)
  end
end

main ()
