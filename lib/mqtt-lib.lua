-- MQTT Lib
-- Author: Navatusein
-- License: MIT
-- Version: 1.3

local internet = require("internet")

---@class MQTTConfig
---@field host string
---@field port number
---@field username string|nil
---@field password string|nil
---@field clientId string|nil
---@field keepAlive integer|nil

---@class ParsedPacket
---@field type number
---@field flags number
---@field payload string

local mqtt = {}

---Crate new List object from config
---@param config MQTTConfig
---@return MQTT
function mqtt:newFormConfig(config)
  return self:new(config.host, config.port, config.username, config.password, config.clientId, config.keepAlive)
end

---Crate new MQTT object
---@param host string
---@param port number
---@param username string|nil
---@param password string|nil
---@param clientId string|nil
---@param keepAlive integer|nil
---@return MQTT
function mqtt:new(host, port, username, password, clientId, keepAlive)

  ---@class MQTT
  local obj = {}

  obj.host = host
  obj.port = port
  obj.username = username
  obj.password = password
  obj.clientId = clientId or "oc-client"
  obj.keepAlive = keepAlive or 60

  obj.onConnect = nil

  obj.socket = nil
  obj.willMessage = nil
  obj.subscriptions = {}
  obj.outbox = {}
  obj.lastPing = 0
  obj.packetId = 0

  ---Register will message
  ---@param topic string
  ---@param message string
  ---@param qos 0|1|2
  ---@param retain boolean
  function obj:registerWillMessage(topic, message, qos, retain)
    if self.connected then
      error("Register will message before connect")
    end

    self.willMessage = {topic = topic, message = message, qos = qos or 0, retain = retain or false}
  end

  ---Register handler on connect
  ---@param onConnect fun()
  function obj:registerOnConnect(onConnect)
    if self.connected then
      error("Register on connect handler before connect")
    end

    self.onConnect = onConnect;
  end

  ---Connect to broker
  function obj:connect()
    self.socket = assert(internet.socket(self.host, self.port))

    self.connected = false

    local flags = 0x82
    local payload = self:encodeString(self.clientId)

    if self.willMessage ~= nil then
      flags = flags + 0x04 + (self.willMessage.qos << 3) + ((self.willMessage.retain and 1 or 0) << 5)

      payload = payload..self:encodeString(self.willMessage.topic)
      payload = payload..self:encodeString(self.willMessage.message)
    end

    if #self.username ~= 0 and #self.password ~= 0 then
      flags = flags + 0x40

      payload = payload..self:encodeString(self.username)
      payload = payload..self:encodeString(self.password)
    end

    local variableHeader = string.char(0x00, 0x04, 0x4D, 0x51, 0x54, 0x54)..
      string.char(0x04)..
      string.char(flags)..
      string.char(math.floor(self.keepAlive/ 256), self.keepAlive % 256)

    self:sendPacket(0x10, variableHeader..payload)

    local packet = self:receivePacket()

    while packet == nil do
      packet = self:receivePacket()
    end

    assert(packet.type ~= 0x20, "Unknown connect response")
    assert(packet.payload:byte(2) == 0, "Connection failed with error code " .. packet.payload:byte(2))

    if self.onConnect ~= nil then
      self.onConnect()
    end

    for _, value in pairs(self.subscriptions) do
      self:sendSubscribePacket(value.topic, value.qos)
    end

    self.connected = true
    self.lastPing = os.time()
  end

  ---Publish message
  ---@param topic string
  ---@param message string
  ---@param qos 0|1|2|nil
  ---@param retain boolean|nil
  function obj:publish(topic, message, qos, retain)
    qos = qos or 0
    retain = retain or false

    local flags = (qos << 1) + (retain and 1 or 0)
    local packetId = qos > 0 and self:getNextPacketId() or nil
    local variableHeader = self:encodeString(topic)

    if packetId then
      variableHeader = variableHeader..string.char(packetId >> 8, packetId & 0xFF)
    end

    self:sendPacket(0x30 + flags, variableHeader..message)

    if qos > 0 and packetId ~= nil then
      self.outbox[packetId] = {topic = topic, message = message, qos = qos, retain = retain}
    end
  end

  ---Subscribe to the topic
  ---@param topic string
  ---@param qos 0|1|2|nil
  ---@param callback fun(topic: string, message: string, topicValues?: string[])
  function obj:subscribe(topic, qos, callback)
    self:sendSubscribePacket(topic, qos)
    self.subscriptions[topic] = {
      topic = topic,
      qos = qos,
      callback = callback;
    }
  end

  ---Loop
  function obj:loop()
    self:checkKeepAlive()
    self:handlePacket()
  end

  ---Send ping message
  ---@private
  function obj:checkKeepAlive()
    if os.time() - self.lastPing >= math.floor(self.keepAlive) * 100 then
      self:sendPacket(0xC0, "")
      self.lastPing = os.time()
    end
  end

  ---Encode string to format 2 byte size other bytes value
  ---@param value string
  ---@return string
  ---@private
  function obj:encodeString(value)
    return string.char(#value >> 8, #value & 0xFF)..value
  end

  ---Generate next packet id
  ---@return integer
  ---@private
  function obj:getNextPacketId()
    self.packetId = (self.packetId + 1) % 0xFFFF
    return self.packetId
  end

  ---Compare mqtt topics
  ---@param topic string
  ---@param messageTopic string
  ---@return boolean
  ---@return string[]
  ---@private
  function obj:compareTopics(topic, messageTopic)
    local topicLevels = {}
    for level in string.gmatch(topic, "([^/]+)") do
        table.insert(topicLevels, level)
    end

    local messageLevels = {}
    for level in string.gmatch(messageTopic, "([^/]+)") do
        table.insert(messageLevels, level)
    end

    if #messageLevels > #topicLevels and topicLevels[#topicLevels] ~= "#" then
        return false, {}
    end

    local extractedValues = {}

    for i = 1, #topicLevels do
        local topicLevel = topicLevels[i]
        local messageLevel = messageLevels[i]

        if topicLevel == "#" then
            local remainingLevels = table.concat(messageLevels, "/", i)
            table.insert(extractedValues, remainingLevels)
            return true, extractedValues
        end

        if topicLevel == "+" then
            table.insert(extractedValues, messageLevel)
        elseif topicLevel ~= messageLevel then
            return false, {}
        end
    end

    return #messageLevels == #topicLevels or topicLevels[#topicLevels] == "#", extractedValues
  end

  ---Send packet
  ---@param type number
  ---@param payload string
  ---@private
  function obj:sendPacket(type, payload)
    local remainingLength = #payload
    local lengthBytes = {}

    repeat
      local byte = remainingLength % 128
      remainingLength = remainingLength // 128

      if remainingLength > 0 then
        byte = byte + 0x80
      end

      table.insert(lengthBytes, string.char(byte))
    until remainingLength == 0

    self.socket:write(string.char(type)..table.concat(lengthBytes)..payload)
  end

  ---Send subscribe packet
  ---@param topic string
  ---@param qos 0|1|2|nil
  ---@private
  function obj:sendSubscribePacket(topic, qos)
    qos = qos or 0

    local packetId = self:getNextPacketId()
    local variableHeader = string.char(packetId >> 8, packetId & 0xFF)
    local payload = self:encodeString(topic)..string.char(qos)

    self:sendPacket(0x82, variableHeader..payload)
  end

  ---Receive packet
  ---@return ParsedPacket|nil
  ---@private
  function obj:receivePacket()
    local header = self.socket:read(1)

    if header == nil then
      self:connect()
      return nil
    end

    if header:byte() == nil then
      return nil
    end

    local type = header:byte() >> 4
    local flags = header:byte() & 0x0F
    local remainingLength = 0
    local multiplier = 1

    repeat
      local byte = self.socket:read(1):byte()
      remainingLength = remainingLength + (byte % 128) * multiplier
      multiplier = multiplier * 128
    until byte < 128

    local payload = self.socket:read(remainingLength)

    return {type = type, flags = flags, payload = payload}
  end

  ---Handle incoming packets
  ---@private
  function obj:handlePacket()
    local packet = self:receivePacket()

    if packet == nil then
      return
    end

    if packet.type == 0x03 then
      self:handlePublish(packet)
    elseif packet.type == 0x04 then
      self:handlePuback(packet)
    elseif packet.type == 0x05 then
      self:handlePubrec(packet)
    elseif packet.type == 0x06 then
      self:handlePubrel(packet)
    elseif packet.type == 0x07 then
      self:handlePubcomp(packet)
    elseif packet.type == 0x09 then
      self:handleSuback(packet)
    end
  end

  ---Handle Publish
  ---@param packet ParsedPacket
  ---@private
  function obj:handlePublish(packet)
    local topicLength = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
    local topic = packet.payload:sub(3, 2 + topicLength)
    local message = packet.payload:sub(3 + topicLength)
    local packetId = nil

    if packet.flags >> 1 & 0x03 > 0 then
      packetId = (message:byte(1) << 8) + message:byte(2)
      message = message:sub(3)
    end

    if self.subscriptions[topic] ~= nil then
      self.subscriptions[topic].callback(topic, message, {})
    end

    for index, value in pairs(self.subscriptions) do
      local isMatch, topicValues = self:compareTopics(index, topic)

      if isMatch then
        value.callback(topic, message, topicValues)
        break
      end
    end

    if packet.flags >> 1 & 0x03 == 1 then
      self:sendPacket(0x40, string.char(packetId >> 8, packetId & 0xFF))
    elseif packet.flags >> 1 & 0x03 == 2 then
      self:sendPacket(0x50, string.char(packetId >> 8, packetId & 0xFF))
    end
  end

  ---Handle Puback
  ---@param packet ParsedPacket
  ---@private
  function obj:handlePuback(packet)
    local packetId = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
    self.outbox[packetId] = nil
  end

  ---Handle Pubrec
  ---@param packet ParsedPacket
  ---@private
  function obj:handlePubrec(packet)
    local packetId = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
    self:sendPacket(0x62, string.char(packetId >> 8, packetId & 0xFF))
  end

  ---Handle Pubrel
  ---@param packet ParsedPacket
  ---@private
  function obj:handlePubrel(packet)
    local packetId = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
    self:sendPacket(0x70, string.char(packetId >> 8, packetId & 0xFF))
    self.outbox[packetId] = nil
  end

  ---Handle Pubcomp
  ---@param packet ParsedPacket
  ---@private
  function obj:handlePubcomp(packet)
    local packetId = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
    self.outbox[packetId] = nil
  end

  ---Handle Suback
  ---@param packet ParsedPacket
  ---@private
  function obj:handleSuback(packet)
    local packetId = (packet.payload:byte(1) << 8) + packet.payload:byte(2)
  end

  setmetatable(obj, self)
  self.__index = self
  return obj
end

return mqtt