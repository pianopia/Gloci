import std/[asynchttpserver, asyncdispatch, strutils, tables, json, os, times, uri]

type
  StoredObject = object
    name: string
    data: string
    updatedAt: int64

  Bucket = ref object
    name: string
    objects: Table[string, StoredObject]

  Topic = ref object
    name: string

  PubsubMessage = object
    messageId: string
    data: string
    attributes: JsonNode
    publishTime: string

  Subscription = ref object
    name: string
    topic: string
    queue: seq[PubsubMessage]

  SchedulerJob = ref object
    name: string
    topic: string
    payload: string
    everySeconds: int
    nextRunEpoch: int64
    lastRunEpoch: int64

  AppState = ref object
    buckets: Table[string, Bucket]
    topics: Table[string, Topic]
    subscriptions: Table[string, Subscription]
    jobs: Table[string, SchedulerJob]
    nextMessageId: uint64

proc newAppState(): AppState =
  AppState(
    buckets: initTable[string, Bucket](),
    topics: initTable[string, Topic](),
    subscriptions: initTable[string, Subscription](),
    jobs: initTable[string, SchedulerJob](),
    nextMessageId: 0'u64
  )

proc nowUnix(): int64 =
  getTime().toUnix()

proc nowIsoUtc(): string =
  getTime().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")

proc splitPath(path: string): seq[string] =
  let normalized = path.strip(chars = {'/'})
  if normalized.len == 0:
    return @[]
  for part in normalized.split("/"):
    if part.len > 0:
      result.add(decodeUrl(part))

proc parseQueryParams(query: string): Table[string, string] =
  result = initTable[string, string]()
  if query.len == 0:
    return
  for pair in query.split("&"):
    if pair.len == 0:
      continue
    let eqPos = pair.find('=')
    if eqPos < 0:
      result[decodeUrl(pair)] = ""
      continue
    let key = decodeUrl(pair[0 ..< eqPos])
    var value = ""
    if eqPos < pair.high:
      value = decodeUrl(pair[eqPos + 1 .. ^1])
    result[key] = value

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

proc pullMessages(subName: string; sub: Subscription; maxMessages: int): JsonNode =
  result = newJArray()
  var count = 0
  while sub.queue.len > 0 and count < maxMessages:
    let msg = sub.queue[0]
    sub.queue.delete(0)
    result.add(%*{
      "ackId": subName & ":" & msg.messageId,
      "message": {
        "messageId": msg.messageId,
        "data": msg.data,
        "attributes": msg.attributes,
        "publishTime": msg.publishTime
      }
    })
    inc count

proc storageObjectMetadata(bucketName: string; obj: StoredObject): JsonNode =
  %*{
    "bucket": bucketName,
    "name": obj.name,
    "size": obj.data.len,
    "updatedAt": obj.updatedAt
  }

proc parseBodyJson(req: Request; payload: var JsonNode; err: var string): bool =
  if req.body.len == 0:
    payload = newJObject()
    return true
  try:
    payload = parseJson(req.body)
    true
  except JsonParsingError as ex:
    err = ex.msg
    false

proc jsonResponse(req: Request; code: HttpCode; payload: JsonNode) {.async, gcsafe.} =
  let headers = newHttpHeaders([("Content-Type", "application/json; charset=utf-8")])
  await req.respond(code, $payload, headers)

proc errorResponse(req: Request; code: HttpCode; message: string) {.async, gcsafe.} =
  await jsonResponse(req, code, %*{"error": message})

proc fanOutMessage(state: AppState; topicName: string; message: PubsubMessage) =
  for _, sub in state.subscriptions.mpairs:
    if sub.topic == topicName:
      sub.queue.add(message)

proc publishMessages(state: AppState; topicName: string; messages: seq[JsonNode]): seq[string] =
  for inputMessage in messages:
    var data = ""
    var attributes = newJObject()
    if inputMessage.kind == JObject:
      if inputMessage.hasKey("data") and inputMessage["data"].kind == JString:
        data = inputMessage["data"].getStr()
      if inputMessage.hasKey("attributes") and inputMessage["attributes"].kind == JObject:
        attributes = inputMessage["attributes"]

    inc state.nextMessageId
    let messageId = $state.nextMessageId
    let message = PubsubMessage(
      messageId: messageId,
      data: data,
      attributes: attributes,
      publishTime: nowIsoUtc()
    )
    state.fanOutMessage(topicName, message)
    result.add(messageId)

proc runSchedulerJob(state: AppState; job: SchedulerJob) =
  if not state.topics.hasKey(job.topic):
    return
  let payload = %*{
    "data": job.payload,
    "attributes": {
      "source": "scheduler",
      "job": job.name
    }
  }
  discard state.publishMessages(job.topic, @[payload])
  let ts = nowUnix()
  job.lastRunEpoch = ts
  job.nextRunEpoch = ts + int64(job.everySeconds)

proc schedulerLoop(state: AppState) {.async, gcsafe.} =
  while true:
    let ts = nowUnix()
    for _, job in state.jobs.mpairs:
      if job.everySeconds > 0 and ts >= job.nextRunEpoch:
        state.runSchedulerJob(job)
    await sleepAsync(1000)

proc handleRequest(req: Request; state: AppState) {.async, gcsafe.} =
  let path = req.url.path
  let parts = splitPath(path)

  if req.reqMethod == HttpGet and path == "/":
    await jsonResponse(req, Http200, %*{"name": "gloci", "status": "running"})
    return

  if req.reqMethod == HttpGet and path == "/healthz":
    await jsonResponse(req, Http200, %*{"status": "ok"})
    return

  # Google Cloud Storage JSON API style compatibility endpoints.
  if parts.len == 3 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "b":
    if req.reqMethod == HttpGet:
      var buckets = newJArray()
      for _, bucket in state.buckets:
        buckets.add(%*{"name": bucket.name})
      await jsonResponse(req, Http200, %*{"items": buckets})
      return

    if req.reqMethod == HttpPost:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("name") or body["name"].kind != JString:
        await errorResponse(req, Http400, "Request must include bucket name")
        return
      let bucketName = body["name"].getStr().strip()
      if bucketName.len == 0:
        await errorResponse(req, Http400, "Bucket name must not be empty")
        return
      if state.buckets.hasKey(bucketName):
        await errorResponse(req, Http409, "Bucket already exists")
        return
      state.buckets[bucketName] = Bucket(name: bucketName, objects: initTable[string, StoredObject]())
      await jsonResponse(req, Http200, %*{"name": bucketName})
      return

  if parts.len == 5 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "b" and parts[4] == "o":
    let bucketName = parts[3]
    if not state.buckets.hasKey(bucketName):
      await errorResponse(req, Http404, "Bucket not found")
      return

    if req.reqMethod == HttpGet:
      var objects = newJArray()
      for _, obj in state.buckets[bucketName].objects:
        objects.add(storageObjectMetadata(bucketName, obj))
      await jsonResponse(req, Http200, %*{"bucket": bucketName, "items": objects})
      return

  if parts.len == 6 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "b" and parts[4] == "o":
    let bucketName = parts[3]
    let objectName = parts[5]
    if not state.buckets.hasKey(bucketName):
      await errorResponse(req, Http404, "Bucket not found")
      return
    if not state.buckets[bucketName].objects.hasKey(objectName):
      await errorResponse(req, Http404, "Object not found")
      return

    if req.reqMethod == HttpGet:
      let queryParams = parseQueryParams(req.url.query)
      let objectData = state.buckets[bucketName].objects[objectName].data
      if queryParams.hasKey("alt") and queryParams["alt"] == "media":
        let headers = newHttpHeaders([("Content-Type", "application/octet-stream")])
        await req.respond(Http200, objectData, headers)
        return
      await jsonResponse(req, Http200, storageObjectMetadata(bucketName, state.buckets[bucketName].objects[objectName]))
      return

  if parts.len == 6 and parts[0] == "upload" and parts[1] == "storage" and parts[2] == "v1" and
      parts[3] == "b" and parts[5] == "o":
    let bucketName = parts[4]
    if req.reqMethod == HttpPost:
      if not state.buckets.hasKey(bucketName):
        await errorResponse(req, Http404, "Bucket not found")
        return
      let queryParams = parseQueryParams(req.url.query)
      if not queryParams.hasKey("name") or queryParams["name"].strip().len == 0:
        await errorResponse(req, Http400, "Query parameter name is required")
        return
      let objectName = queryParams["name"]
      let objectData = req.body
      state.buckets[bucketName].objects[objectName] = StoredObject(
        name: objectName,
        data: objectData,
        updatedAt: nowUnix()
      )
      await jsonResponse(req, Http200, storageObjectMetadata(bucketName, state.buckets[bucketName].objects[objectName]))
      return

  if parts.len == 7 and parts[0] == "download" and parts[1] == "storage" and parts[2] == "v1" and
      parts[3] == "b" and parts[5] == "o":
    let bucketName = parts[4]
    let objectName = parts[6]
    if req.reqMethod == HttpGet:
      if not state.buckets.hasKey(bucketName):
        await errorResponse(req, Http404, "Bucket not found")
        return
      if not state.buckets[bucketName].objects.hasKey(objectName):
        await errorResponse(req, Http404, "Object not found")
        return
      let objectData = state.buckets[bucketName].objects[objectName].data
      let headers = newHttpHeaders([("Content-Type", "application/octet-stream")])
      await req.respond(Http200, objectData, headers)
      return

  # Google Cloud Pub/Sub style compatibility endpoints.
  if parts.len == 4 and parts[0] == "v1" and parts[1] == "projects" and parts[3] == "topics":
    let projectId = parts[2]
    if req.reqMethod == HttpGet:
      var topics = newJArray()
      for _, topic in state.topics:
        topics.add(%*{"name": pubsubTopicFullName(projectId, topic.name)})
      await jsonResponse(req, Http200, %*{"topics": topics})
      return

  if parts.len == 5 and parts[0] == "v1" and parts[1] == "projects" and parts[3] == "topics":
    let projectId = parts[2]
    if req.reqMethod == HttpPut:
      let topicName = parts[4]
      if topicName.len == 0 or topicName.contains(":"):
        await errorResponse(req, Http400, "Invalid topic name")
        return
      if state.topics.hasKey(topicName):
        await errorResponse(req, Http409, "Topic already exists")
        return
      state.topics[topicName] = Topic(name: topicName)
      await jsonResponse(req, Http200, %*{"name": pubsubTopicFullName(projectId, topicName)})
      return

    if req.reqMethod == HttpPost and parts[4].endsWith(":publish"):
      let topicName = parts[4][0 ..< parts[4].len - ":publish".len]
      if topicName.len == 0:
        await errorResponse(req, Http400, "Invalid topic name")
        return
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("messages") or body["messages"].kind != JArray:
        await errorResponse(req, Http400, "Request must include messages array")
        return

      var messages: seq[JsonNode] = @[]
      for message in body["messages"].items:
        messages.add(message)
      let ids = state.publishMessages(topicName, messages)
      await jsonResponse(req, Http200, %*{"messageIds": ids})
      return

  if parts.len == 4 and parts[0] == "v1" and parts[1] == "projects" and parts[3] == "subscriptions":
    let projectId = parts[2]
    if req.reqMethod == HttpGet:
      var subscriptions = newJArray()
      for _, sub in state.subscriptions:
        subscriptions.add(%*{
          "name": pubsubSubscriptionFullName(projectId, sub.name),
          "topic": pubsubTopicFullName(projectId, sub.topic)
        })
      await jsonResponse(req, Http200, %*{"subscriptions": subscriptions})
      return

  if parts.len == 5 and parts[0] == "v1" and parts[1] == "projects" and parts[3] == "subscriptions":
    let projectId = parts[2]
    if req.reqMethod == HttpPut:
      let subName = parts[4]
      if subName.len == 0 or subName.contains(":"):
        await errorResponse(req, Http400, "Invalid subscription name")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("topic") or body["topic"].kind != JString:
        await errorResponse(req, Http400, "Request must include topic")
        return

      var topicName = ""
      if not parsePubsubResourceName(body["topic"].getStr(), "topics", topicName):
        await errorResponse(req, Http400, "topic must be topic name or projects/{project}/topics/{topic}")
        return
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return
      if state.subscriptions.hasKey(subName):
        await errorResponse(req, Http409, "Subscription already exists")
        return
      state.subscriptions[subName] = Subscription(name: subName, topic: topicName, queue: @[])
      await jsonResponse(req, Http200, %*{
        "name": pubsubSubscriptionFullName(projectId, subName),
        "topic": pubsubTopicFullName(projectId, topicName)
      })
      return

    if req.reqMethod == HttpPost and parts[4].endsWith(":pull"):
      let subName = parts[4][0 ..< parts[4].len - ":pull".len]
      if subName.len == 0:
        await errorResponse(req, Http400, "Invalid subscription name")
        return
      if not state.subscriptions.hasKey(subName):
        await errorResponse(req, Http404, "Subscription not found")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return

      var maxMessages = 1
      var maxErr = ""
      if not parseMaxMessages(body, maxMessages, maxErr):
        await errorResponse(req, Http400, maxErr)
        return
      let pulled = pullMessages(subName, state.subscriptions[subName], maxMessages)
      await jsonResponse(req, Http200, %*{"receivedMessages": pulled})
      return

    if req.reqMethod == HttpPost and parts[4].endsWith(":acknowledge"):
      let subName = parts[4][0 ..< parts[4].len - ":acknowledge".len]
      if subName.len == 0:
        await errorResponse(req, Http400, "Invalid subscription name")
        return
      if not state.subscriptions.hasKey(subName):
        await errorResponse(req, Http404, "Subscription not found")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if body.hasKey("ackIds") and body["ackIds"].kind != JArray:
        await errorResponse(req, Http400, "ackIds must be array")
        return
      await jsonResponse(req, Http200, %*{})
      return

  if parts.len == 3 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "buckets":
    if req.reqMethod == HttpGet:
      var buckets = newJArray()
      for _, bucket in state.buckets:
        buckets.add(%*{
          "name": bucket.name,
          "objectCount": bucket.objects.len
        })
      await jsonResponse(req, Http200, %*{"buckets": buckets})
      return

  if parts.len == 4 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "buckets":
    let bucketName = parts[3]
    if req.reqMethod == HttpPut:
      if state.buckets.hasKey(bucketName):
        await errorResponse(req, Http409, "Bucket already exists")
        return
      state.buckets[bucketName] = Bucket(name: bucketName, objects: initTable[string, StoredObject]())
      await jsonResponse(req, Http201, %*{"name": bucketName})
      return

  if parts.len == 5 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "buckets" and parts[4] == "objects":
    let bucketName = parts[3]
    if not state.buckets.hasKey(bucketName):
      await errorResponse(req, Http404, "Bucket not found")
      return

    if req.reqMethod == HttpGet:
      var objects = newJArray()
      for _, obj in state.buckets[bucketName].objects:
        objects.add(%*{
          "name": obj.name,
          "size": obj.data.len,
          "updatedAt": obj.updatedAt
        })
      await jsonResponse(req, Http200, %*{"bucket": bucketName, "objects": objects})
      return

  if parts.len == 6 and parts[0] == "storage" and parts[1] == "v1" and parts[2] == "buckets" and parts[4] == "objects":
    let bucketName = parts[3]
    let objectName = parts[5]
    if not state.buckets.hasKey(bucketName):
      await errorResponse(req, Http404, "Bucket not found")
      return

    if req.reqMethod == HttpPut:
      let objectData = req.body
      state.buckets[bucketName].objects[objectName] = StoredObject(
        name: objectName,
        data: objectData,
        updatedAt: nowUnix()
      )
      await jsonResponse(req, Http201, %*{
        "bucket": bucketName,
        "name": objectName,
        "size": objectData.len
      })
      return

    if req.reqMethod == HttpGet:
      if not state.buckets[bucketName].objects.hasKey(objectName):
        await errorResponse(req, Http404, "Object not found")
        return
      let objectData = state.buckets[bucketName].objects[objectName].data
      let headers = newHttpHeaders([("Content-Type", "application/octet-stream")])
      await req.respond(Http200, objectData, headers)
      return

  if parts.len == 3 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "topics":
    if req.reqMethod == HttpGet:
      var topics = newJArray()
      for _, topic in state.topics:
        topics.add(%*{"name": topic.name})
      await jsonResponse(req, Http200, %*{"topics": topics})
      return

  if parts.len == 4 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "topics":
    let topicName = parts[3]
    if req.reqMethod == HttpPut:
      if state.topics.hasKey(topicName):
        await errorResponse(req, Http409, "Topic already exists")
        return
      state.topics[topicName] = Topic(name: topicName)
      await jsonResponse(req, Http201, %*{"name": topicName})
      return

  if parts.len == 5 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "topics" and parts[4] == "publish":
    let topicName = parts[3]
    if req.reqMethod == HttpPost:
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("messages") or body["messages"].kind != JArray:
        await errorResponse(req, Http400, "Request must include messages array")
        return

      var messages: seq[JsonNode] = @[]
      for message in body["messages"].items:
        messages.add(message)
      let ids = state.publishMessages(topicName, messages)
      await jsonResponse(req, Http200, %*{"messageIds": ids})
      return

  if parts.len == 3 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "subscriptions":
    if req.reqMethod == HttpGet:
      var subscriptions = newJArray()
      for _, sub in state.subscriptions:
        subscriptions.add(%*{
          "name": sub.name,
          "topic": sub.topic,
          "queuedMessages": sub.queue.len
        })
      await jsonResponse(req, Http200, %*{"subscriptions": subscriptions})
      return

  if parts.len == 4 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "subscriptions":
    let subName = parts[3]
    if req.reqMethod == HttpPut:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("topic") or body["topic"].kind != JString:
        await errorResponse(req, Http400, "Request must include topic")
        return

      let topicName = body["topic"].getStr()
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return
      if state.subscriptions.hasKey(subName):
        await errorResponse(req, Http409, "Subscription already exists")
        return
      state.subscriptions[subName] = Subscription(name: subName, topic: topicName, queue: @[])
      await jsonResponse(req, Http201, %*{"name": subName, "topic": topicName})
      return

  if parts.len == 5 and parts[0] == "pubsub" and parts[1] == "v1" and parts[2] == "subscriptions" and parts[4] == "pull":
    let subName = parts[3]
    if req.reqMethod == HttpPost:
      if not state.subscriptions.hasKey(subName):
        await errorResponse(req, Http404, "Subscription not found")
        return
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return

      var maxMessages = 1
      var maxErr = ""
      if not parseMaxMessages(body, maxMessages, maxErr):
        await errorResponse(req, Http400, maxErr)
        return
      let pulled = pullMessages(subName, state.subscriptions[subName], maxMessages)
      await jsonResponse(req, Http200, %*{"receivedMessages": pulled})
      return

  if parts.len == 3 and parts[0] == "scheduler" and parts[1] == "v1" and parts[2] == "jobs":
    if req.reqMethod == HttpGet:
      var jobs = newJArray()
      for _, job in state.jobs:
        jobs.add(%*{
          "name": job.name,
          "topic": job.topic,
          "payload": job.payload,
          "everySeconds": job.everySeconds,
          "nextRunEpoch": job.nextRunEpoch,
          "lastRunEpoch": job.lastRunEpoch
        })
      await jsonResponse(req, Http200, %*{"jobs": jobs})
      return

  if parts.len == 4 and parts[0] == "scheduler" and parts[1] == "v1" and parts[2] == "jobs":
    let jobName = parts[3]
    if req.reqMethod == HttpPut:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("topic") or body["topic"].kind != JString:
        await errorResponse(req, Http400, "Request must include topic")
        return
      if not body.hasKey("everySeconds") or body["everySeconds"].kind != JInt:
        await errorResponse(req, Http400, "Request must include integer everySeconds")
        return

      let topicName = body["topic"].getStr()
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return

      let everySeconds = body["everySeconds"].getInt()
      if everySeconds < 1:
        await errorResponse(req, Http400, "everySeconds must be >= 1")
        return

      var payload = ""
      if body.hasKey("payload") and body["payload"].kind == JString:
        payload = body["payload"].getStr()

      let ts = nowUnix()
      state.jobs[jobName] = SchedulerJob(
        name: jobName,
        topic: topicName,
        payload: payload,
        everySeconds: everySeconds,
        nextRunEpoch: ts + int64(everySeconds),
        lastRunEpoch: 0
      )
      await jsonResponse(req, Http201, %*{
        "name": jobName,
        "topic": topicName,
        "everySeconds": everySeconds
      })
      return

  if parts.len == 5 and parts[0] == "scheduler" and parts[1] == "v1" and parts[2] == "jobs" and parts[4] == "run":
    let jobName = parts[3]
    if req.reqMethod == HttpPost:
      if not state.jobs.hasKey(jobName):
        await errorResponse(req, Http404, "Job not found")
        return
      state.runSchedulerJob(state.jobs[jobName])
      await jsonResponse(req, Http200, %*{"name": jobName, "status": "triggered"})
      return

  await errorResponse(req, Http404, "Not found")

proc main() =
  let state = newAppState()
  asyncCheck schedulerLoop(state)

  let server = newAsyncHttpServer()
  let portStr = getEnv("GLOCI_PORT", "8080")
  var port = Port(8080)
  try:
    port = Port(parseInt(portStr))
  except ValueError:
    echo "Invalid GLOCI_PORT value: " & portStr & ", fallback to 8080"

  proc cb(req: Request) {.async, gcsafe.} =
    await handleRequest(req, state)

  echo "gloci listening on http://0.0.0.0:" & $port.int
  waitFor server.serve(port, cb)

when isMainModule:
  main()
