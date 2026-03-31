proc reclaimExpiredInFlight(sub: Subscription; nowEpoch: int64): bool =
  var expiredAckIds: seq[string] = @[]
  for ackId, inFlight in sub.inFlight:
    if inFlight.deadlineEpoch <= nowEpoch:
      expiredAckIds.add(ackId)
  for ackId in expiredAckIds:
    if sub.inFlight.hasKey(ackId):
      sub.queue.add(sub.inFlight[ackId].message)
      sub.inFlight.del(ackId)
  expiredAckIds.len > 0

proc reclaimExpiredAckDeadlines(state: AppState; nowEpoch: int64): bool =
  for _, sub in state.subscriptions.mpairs:
    if reclaimExpiredInFlight(sub, nowEpoch):
      result = true

proc generateAckId(state: AppState; subName: string): string =
  inc state.nextAckId
  subName & ":" & $state.nextAckId

proc pullMessages(state: AppState; subName: string; sub: Subscription; maxMessages: int): JsonNode =
  let ts = nowUnix()
  var changed = reclaimExpiredInFlight(sub, ts)

  result = newJArray()
  var count = 0
  while sub.queue.len > 0 and count < maxMessages:
    let msg = sub.queue[0]
    sub.queue.delete(0)
    let ackId = state.generateAckId(subName)
    sub.inFlight[ackId] = InFlightMessage(
      message: msg,
      deadlineEpoch: ts + int64(sub.ackDeadlineSeconds)
    )
    result.add(%*{
      "ackId": ackId,
      "message": pubsubMessageToJson(msg)
    })
    inc count
    changed = true

  if changed:
    state.persistState()

proc acknowledgeMessages(sub: Subscription; ackIds: seq[string]): int =
  for ackId in ackIds:
    if sub.inFlight.hasKey(ackId):
      sub.inFlight.del(ackId)
      inc result

proc modifyAckDeadlines(sub: Subscription; ackIds: seq[string]; ackDeadlineSeconds: int): int =
  let nowEpoch = nowUnix()
  for ackId in ackIds:
    if not sub.inFlight.hasKey(ackId):
      continue
    let inFlight = sub.inFlight[ackId]
    if ackDeadlineSeconds == 0:
      sub.queue.add(inFlight.message)
      sub.inFlight.del(ackId)
      inc result
      continue
    sub.inFlight[ackId] = InFlightMessage(
      message: inFlight.message,
      deadlineEpoch: nowEpoch + int64(ackDeadlineSeconds)
    )
    inc result

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
  if result.len > 0:
    state.persistState()

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
  if job.everySeconds > 0:
    job.nextRunEpoch = ts + int64(job.everySeconds)
  elif job.cron.expression.len > 0:
    job.nextRunEpoch = nextCronRunEpoch(job.cron, ts)
  else:
    job.nextRunEpoch = 0

proc schedulerLoop(state: AppState) {.async, gcsafe.} =
  while true:
    let ts = nowUnix()
    if state.reclaimExpiredAckDeadlines(ts):
      state.persistState()
    for _, job in state.jobs.mpairs:
      if job.nextRunEpoch > 0 and ts >= job.nextRunEpoch:
        state.runSchedulerJob(job)
    await sleepAsync(1000)

