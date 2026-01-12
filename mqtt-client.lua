local colors = require("colors")
local component = require("component")
local computer = require("computer")
local event = require "event"
local internet = require("internet")
local json = require("json")
local serialization = require("serialization")
local sides = require("sides")

local cfg = require("config")

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


local mqtt = mqttLib:new(cfg["server"], 1883, cfg["username"], cfg["password"], computer.address(), 600)
local avail_topic = "oc-computer/"..computer.address().."/online"

local function get_device(typ, addr)
  local dev = {}
  dev["name"] = "opencomputer " .. typ .. " " .. addr
  dev["serial_number"] = addr
  dev["identifiers"] = { addr }
  return dev
end

local function register_redstone(addr)
  local dev = component.proxy(addr)
  for i=0,15,1 do
    local config = {}
    local c = colors[i]
    config["device"] = get_device("redstone", addr)
    config["command_topic"] = "oc-computer/" .. addr .. "/" .. i .. "/set"
    config["name"] = "east redstone color " .. colors[i]
    config["platform"] = "switch"
    config["unique_id"] = addr .. "/" .. i

    mqtt:publish("homeassistant/switch/" .. addr .. "/" .. i .. "/config", json:encode(config))
  end
end

local function register_transposer_fluid(addr, name, label)
  local dev = component.proxy(addr)

  local avail = {}
  avail["topic"] = avail_topic

  local config = {}
  local topic = "oc-computer/" .. addr .. "/report"
  config["device"] = get_device("transposer", addr)
  config["device_class"] = "volume"
  config["availability"] = avail
  config["name"] = label
  config["platform"] = "sensor"
  config["state_topic"] = topic
  config["unique_id"] = addr .. "/" .. name
  config["unit_of_measurement"] = "mL"
  config["value_template"] = "{{ value_json." .. name .. " }}"
  mqtt:publish("homeassistant/sensor/" .. addr .. "_" .. name .. "/config", json:encode(config))
end

local registered_fluids = {}

local function scan_transposer(addr)
  local dev = component.proxy(addr)
  local report = {}
  local names = {}
  for side = 0,5,1 do
    local sidename = sides[side]
    local o = dev.getFluidInTank(side)
    if #o > 0 then
      for k,v in ipairs(o) do
        if v["name"] ~= nil then
          local key = v["name"]:gsub("%s","_")
          names[key] = v["label"]
          if report[key] == nil then
            report[key] = v["amount"]
          else
            report[key] = report[key] + v["amount"]
          end
        end
      end
    end
  end
  for k,v in pairs(report) do
    if registered_fluids[k] == nil then
      print("new fluid to report to HA: " .. names[k])
      register_transposer_fluid(addr, k, names[k])
      registered_fluids[k] = true
    end
  end
  mqtt:publish("oc-computer/"..addr.."/report", json:encode(report))
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
  for k,v in component.list() do
    if v == "transposer" then
      scan_transposer(k)
    end
  end
  mqtt:loop()
end

local function main()
  print("start")
  mqtt:registerWillMessage(avail_topic, "offline", 0, true)
  mqtt:registerOnConnect(function ()
    print("connected")
  end)
  mqtt:connect()
  loop()
  mqtt:publish(avail_topic, "online", 0, true)
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
