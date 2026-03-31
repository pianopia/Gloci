proc persistState(state: AppState) =
  if state.persistencePath.len == 0:
    return

  var persisted = newJObject()
  persisted["version"] = %1
  persisted["nextMessageId"] = %($state.nextMessageId)
  persisted["nextAckId"] = %($state.nextAckId)

  var buckets = newJArray()
  for _, bucket in state.buckets:
    var objects = newJArray()
    for _, obj in bucket.objects:
      objects.add(%*{
        "name": obj.name,
        "data": obj.data,
        "updatedAt": obj.updatedAt
      })
    buckets.add(%*{
      "name": bucket.name,
      "objects": objects
    })
  persisted["buckets"] = buckets

  var topics = newJArray()
  for _, topic in state.topics:
    topics.add(%*{"name": topic.name})
  persisted["topics"] = topics

  var subscriptions = newJArray()
  for _, sub in state.subscriptions:
    var queue = newJArray()
    for msg in sub.queue:
      queue.add(pubsubMessageToJson(msg))
    var inFlightEntries = newJArray()
    for ackId, inFlight in sub.inFlight:
      inFlightEntries.add(%*{
        "ackId": ackId,
        "deadlineEpoch": inFlight.deadlineEpoch,
        "message": pubsubMessageToJson(inFlight.message)
      })
    subscriptions.add(%*{
      "name": sub.name,
      "topic": sub.topic,
      "ackDeadlineSeconds": sub.ackDeadlineSeconds,
      "queue": queue,
      "inFlight": inFlightEntries
    })
  persisted["subscriptions"] = subscriptions

  var bigQueryDatasets = newJArray()
  for _, dataset in state.bqDatasets:
    var labels = newJObject()
    for key, value in dataset.labels:
      labels[key] = %value

    var tables = newJArray()
    for _, table in dataset.tables:
      var schema = newJArray()
      for field in table.schema:
        schema.add(%*{
          "name": field.name,
          "type": field.fieldType,
          "mode": field.mode
        })

      var rows = newJArray()
      for row in table.rows:
        var rowNode = %*{
          "insertId": row.insertId,
          "insertedAt": row.insertedAt
        }
        rowNode["data"] = row.data
        rows.add(rowNode)

      tables.add(%*{
        "projectId": table.projectId,
        "datasetId": table.datasetId,
        "tableId": table.tableId,
        "createdAt": table.createdAt,
        "updatedAt": table.updatedAt,
        "schema": schema,
        "rows": rows
      })

    bigQueryDatasets.add(%*{
      "projectId": dataset.projectId,
      "datasetId": dataset.datasetId,
      "location": dataset.location,
      "createdAt": dataset.createdAt,
      "updatedAt": dataset.updatedAt,
      "labels": labels,
      "tables": tables
    })
  persisted["bigQueryDatasets"] = bigQueryDatasets

  try:
    let dataDir = splitFile(state.persistencePath).dir
    if dataDir.len > 0 and not dirExists(dataDir):
      createDir(dataDir)
    writeFile(state.persistencePath, $persisted)
  except OSError as ex:
    echo "Failed to persist state to " & state.persistencePath & ": " & ex.msg

proc loadState(state: AppState) =
  if state.persistencePath.len == 0 or not fileExists(state.persistencePath):
    return

  try:
    let persisted = parseJson(readFile(state.persistencePath))
    if persisted.kind != JObject:
      echo "Persisted state is invalid JSON object: " & state.persistencePath
      return

    if persisted.hasKey("nextMessageId"):
      let messageIdNode = persisted["nextMessageId"]
      if messageIdNode.kind == JString:
        try:
          state.nextMessageId = parseBiggestUInt(messageIdNode.getStr())
        except ValueError:
          discard
      elif messageIdNode.kind == JInt:
        let raw = messageIdNode.getBiggestInt()
        if raw >= 0:
          state.nextMessageId = uint64(raw)

    if persisted.hasKey("nextAckId"):
      let ackIdNode = persisted["nextAckId"]
      if ackIdNode.kind == JString:
        try:
          state.nextAckId = parseBiggestUInt(ackIdNode.getStr())
        except ValueError:
          discard
      elif ackIdNode.kind == JInt:
        let raw = ackIdNode.getBiggestInt()
        if raw >= 0:
          state.nextAckId = uint64(raw)

    if persisted.hasKey("buckets") and persisted["buckets"].kind == JArray:
      for bucketNode in persisted["buckets"].items:
        if bucketNode.kind != JObject:
          continue
        if not bucketNode.hasKey("name") or bucketNode["name"].kind != JString:
          continue
        let bucketName = bucketNode["name"].getStr().strip()
        if bucketName.len == 0:
          continue
        let bucket = Bucket(name: bucketName, objects: initTable[string, StoredObject]())
        if bucketNode.hasKey("objects") and bucketNode["objects"].kind == JArray:
          for objectNode in bucketNode["objects"].items:
            if objectNode.kind != JObject:
              continue
            if not objectNode.hasKey("name") or objectNode["name"].kind != JString:
              continue
            if not objectNode.hasKey("data") or objectNode["data"].kind != JString:
              continue
            var updatedAt: int64 = 0
            if objectNode.hasKey("updatedAt") and objectNode["updatedAt"].kind == JInt:
              updatedAt = int64(objectNode["updatedAt"].getBiggestInt())
            let objectName = objectNode["name"].getStr()
            bucket.objects[objectName] = StoredObject(
              name: objectName,
              data: objectNode["data"].getStr(),
              updatedAt: updatedAt
            )
        state.buckets[bucketName] = bucket

    if persisted.hasKey("topics") and persisted["topics"].kind == JArray:
      for topicNode in persisted["topics"].items:
        if topicNode.kind != JObject:
          continue
        if not topicNode.hasKey("name") or topicNode["name"].kind != JString:
          continue
        let topicName = topicNode["name"].getStr().strip()
        if topicName.len == 0:
          continue
        state.topics[topicName] = Topic(name: topicName)

    if persisted.hasKey("subscriptions") and persisted["subscriptions"].kind == JArray:
      for subNode in persisted["subscriptions"].items:
        if subNode.kind != JObject:
          continue
        if not subNode.hasKey("name") or subNode["name"].kind != JString:
          continue
        if not subNode.hasKey("topic") or subNode["topic"].kind != JString:
          continue

        let subName = subNode["name"].getStr().strip()
        let topicName = subNode["topic"].getStr().strip()
        if subName.len == 0 or topicName.len == 0:
          continue
        if not state.topics.hasKey(topicName):
          continue

        var ackDeadlineSeconds = 10
        if subNode.hasKey("ackDeadlineSeconds") and subNode["ackDeadlineSeconds"].kind == JInt:
          let parsed = subNode["ackDeadlineSeconds"].getInt()
          if parsed >= 1 and parsed <= 600:
            ackDeadlineSeconds = parsed

        var queue: seq[PubsubMessage] = @[]
        if subNode.hasKey("queue") and subNode["queue"].kind == JArray:
          for queueNode in subNode["queue"].items:
            var message: PubsubMessage
            if parsePubsubMessage(queueNode, message):
              queue.add(message)

        var inFlight = initTable[string, InFlightMessage]()
        if subNode.hasKey("inFlight") and subNode["inFlight"].kind == JArray:
          for inFlightNode in subNode["inFlight"].items:
            if inFlightNode.kind != JObject:
              continue
            if not inFlightNode.hasKey("ackId") or inFlightNode["ackId"].kind != JString:
              continue
            if not inFlightNode.hasKey("message"):
              continue
            let ackId = inFlightNode["ackId"].getStr().strip()
            if ackId.len == 0:
              continue
            var message: PubsubMessage
            if not parsePubsubMessage(inFlightNode["message"], message):
              continue
            var deadlineEpoch: int64 = nowUnix() + int64(ackDeadlineSeconds)
            if inFlightNode.hasKey("deadlineEpoch") and inFlightNode["deadlineEpoch"].kind == JInt:
              deadlineEpoch = int64(inFlightNode["deadlineEpoch"].getBiggestInt())
            inFlight[ackId] = InFlightMessage(message: message, deadlineEpoch: deadlineEpoch)

        state.subscriptions[subName] = Subscription(
          name: subName,
          topic: topicName,
          ackDeadlineSeconds: ackDeadlineSeconds,
          queue: queue,
          inFlight: inFlight
        )

    if persisted.hasKey("bigQueryDatasets") and persisted["bigQueryDatasets"].kind == JArray:
      for datasetNode in persisted["bigQueryDatasets"].items:
        if datasetNode.kind != JObject:
          continue
        if not datasetNode.hasKey("projectId") or datasetNode["projectId"].kind != JString:
          continue
        if not datasetNode.hasKey("datasetId") or datasetNode["datasetId"].kind != JString:
          continue

        let projectId = datasetNode["projectId"].getStr().strip()
        let datasetId = datasetNode["datasetId"].getStr().strip()
        if projectId.len == 0 or not isValidBigQueryIdentifier(datasetId):
          continue

        var location = "US"
        if datasetNode.hasKey("location") and datasetNode["location"].kind == JString:
          location = datasetNode["location"].getStr()

        var createdAt = nowUnix()
        if datasetNode.hasKey("createdAt") and datasetNode["createdAt"].kind == JInt:
          createdAt = int64(datasetNode["createdAt"].getBiggestInt())
        var updatedAt = createdAt
        if datasetNode.hasKey("updatedAt") and datasetNode["updatedAt"].kind == JInt:
          updatedAt = int64(datasetNode["updatedAt"].getBiggestInt())

        var labels = initTable[string, string]()
        if datasetNode.hasKey("labels") and datasetNode["labels"].kind == JObject:
          for key, value in datasetNode["labels"].pairs:
            if value.kind == JString:
              labels[key] = value.getStr()

        let dataset = BigQueryDataset(
          projectId: projectId,
          datasetId: datasetId,
          location: location,
          labels: labels,
          tables: initTable[string, BigQueryTable](),
          createdAt: createdAt,
          updatedAt: updatedAt
        )

        if datasetNode.hasKey("tables") and datasetNode["tables"].kind == JArray:
          for tableNode in datasetNode["tables"].items:
            if tableNode.kind != JObject:
              continue
            if not tableNode.hasKey("tableId") or tableNode["tableId"].kind != JString:
              continue
            let tableId = tableNode["tableId"].getStr().strip()
            if not isValidBigQueryIdentifier(tableId):
              continue

            var schema: seq[BigQueryField] = @[]
            if tableNode.hasKey("schema") and tableNode["schema"].kind == JArray:
              for fieldNode in tableNode["schema"].items:
                if fieldNode.kind != JObject:
                  continue
                if not fieldNode.hasKey("name") or fieldNode["name"].kind != JString:
                  continue
                if not fieldNode.hasKey("type") or fieldNode["type"].kind != JString:
                  continue
                let fieldName = fieldNode["name"].getStr().strip()
                let fieldType = normalizeBigQueryFieldType(fieldNode["type"].getStr())
                if not isValidBigQueryIdentifier(fieldName) or not isSupportedBigQueryFieldType(fieldType):
                  continue
                var mode = "NULLABLE"
                if fieldNode.hasKey("mode") and fieldNode["mode"].kind == JString:
                  let modeValue = fieldNode["mode"].getStr().strip().toUpperAscii()
                  if modeValue == "NULLABLE" or modeValue == "REQUIRED":
                    mode = modeValue
                schema.add(BigQueryField(name: fieldName, fieldType: fieldType, mode: mode))

            var rows: seq[BigQueryRow] = @[]
            var seenInsertIds = initTable[string, int64]()
            if tableNode.hasKey("rows") and tableNode["rows"].kind == JArray:
              for rowNode in tableNode["rows"].items:
                if rowNode.kind != JObject:
                  continue
                if not rowNode.hasKey("data") or rowNode["data"].kind != JObject:
                  continue
                var insertId = ""
                if rowNode.hasKey("insertId") and rowNode["insertId"].kind == JString:
                  insertId = rowNode["insertId"].getStr().strip()
                var insertedAt = nowUnix()
                if rowNode.hasKey("insertedAt") and rowNode["insertedAt"].kind == JInt:
                  insertedAt = int64(rowNode["insertedAt"].getBiggestInt())

                rows.add(BigQueryRow(insertId: insertId, data: rowNode["data"], insertedAt: insertedAt))
                if insertId.len > 0:
                  seenInsertIds[insertId] = insertedAt

            var tableCreatedAt = nowUnix()
            if tableNode.hasKey("createdAt") and tableNode["createdAt"].kind == JInt:
              tableCreatedAt = int64(tableNode["createdAt"].getBiggestInt())
            var tableUpdatedAt = tableCreatedAt
            if tableNode.hasKey("updatedAt") and tableNode["updatedAt"].kind == JInt:
              tableUpdatedAt = int64(tableNode["updatedAt"].getBiggestInt())

            dataset.tables[tableId] = BigQueryTable(
              projectId: projectId,
              datasetId: datasetId,
              tableId: tableId,
              schema: schema,
              rows: rows,
              seenInsertIds: seenInsertIds,
              createdAt: tableCreatedAt,
              updatedAt: tableUpdatedAt
            )

        state.bqDatasets[bigQueryDatasetKey(projectId, datasetId)] = dataset
  except CatchableError as ex:
    echo "Failed to load persisted state from " & state.persistencePath & ": " & ex.msg

