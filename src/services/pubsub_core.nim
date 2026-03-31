proc pubsubTopicFullName(projectId: string; topicName: string): string =
  "projects/" & projectId & "/topics/" & topicName

proc pubsubSubscriptionFullName(projectId: string; subName: string): string =
  "projects/" & projectId & "/subscriptions/" & subName

proc parsePubsubResourceName(value: string; collection: string; name: var string): bool =
  let trimmed = value.strip()
  if trimmed.len == 0:
    return false
  let resourceParts = splitPath(trimmed)
  if resourceParts.len == 1:
    name = resourceParts[0]
    return true
  if resourceParts.len == 4 and resourceParts[0] == "projects" and resourceParts[2] == collection:
    name = resourceParts[3]
    return true
  false

proc parseMaxMessages(body: JsonNode; maxMessages: var int; err: var string): bool =
  maxMessages = 1
  if body.hasKey("maxMessages"):
    if body["maxMessages"].kind != JInt:
      err = "maxMessages must be integer"
      return false
    maxMessages = body["maxMessages"].getInt()
  if maxMessages < 1:
    maxMessages = 1
  true

proc parseSubscriptionAckDeadline(body: JsonNode; ackDeadlineSeconds: var int; err: var string): bool =
  ackDeadlineSeconds = 10
  if body.hasKey("ackDeadlineSeconds"):
    if body["ackDeadlineSeconds"].kind != JInt:
      err = "ackDeadlineSeconds must be integer"
      return false
    ackDeadlineSeconds = body["ackDeadlineSeconds"].getInt()
  if ackDeadlineSeconds < 1 or ackDeadlineSeconds > 600:
    err = "ackDeadlineSeconds must be between 1 and 600"
    return false
  true

proc parseAckIds(body: JsonNode; ackIds: var seq[string]; err: var string): bool =
  if not body.hasKey("ackIds") or body["ackIds"].kind != JArray:
    err = "ackIds must be array"
    return false
  for ackIdNode in body["ackIds"].items:
    if ackIdNode.kind != JString:
      err = "ackIds must contain strings"
      return false
    let ackId = ackIdNode.getStr().strip()
    if ackId.len > 0:
      ackIds.add(ackId)
  true

proc parseModifyAckDeadline(body: JsonNode; ackDeadlineSeconds: var int; err: var string): bool =
  if not body.hasKey("ackDeadlineSeconds") or body["ackDeadlineSeconds"].kind != JInt:
    err = "ackDeadlineSeconds must be integer"
    return false
  ackDeadlineSeconds = body["ackDeadlineSeconds"].getInt()
  if ackDeadlineSeconds < 0 or ackDeadlineSeconds > 600:
    err = "ackDeadlineSeconds must be between 0 and 600"
    return false
  true

proc parsePubsubMessage(node: JsonNode; message: var PubsubMessage): bool =
  if node.kind != JObject:
    return false
  if not node.hasKey("messageId") or node["messageId"].kind != JString:
    return false
  message.messageId = node["messageId"].getStr()
  message.data = ""
  if node.hasKey("data") and node["data"].kind == JString:
    message.data = node["data"].getStr()
  message.attributes = newJObject()
  if node.hasKey("attributes") and node["attributes"].kind == JObject:
    message.attributes = node["attributes"]
  message.publishTime = ""
  if node.hasKey("publishTime") and node["publishTime"].kind == JString:
    message.publishTime = node["publishTime"].getStr()
  true

proc pubsubMessageToJson(message: PubsubMessage): JsonNode =
  %*{
    "messageId": message.messageId,
    "data": message.data,
    "attributes": message.attributes,
    "publishTime": message.publishTime
  }

