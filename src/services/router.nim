proc handleRequest(req: Request; state: AppState) {.async, gcsafe.} =
  let path = req.url.path
  let parts = splitPath(path)

  if req.reqMethod == HttpGet and path == "/":
    await jsonResponse(req, Http200, %*{"name": "gloci", "status": "running"})
    return

  if req.reqMethod == HttpGet and path == "/healthz":
    await jsonResponse(req, Http200, %*{"status": "ok"})
    return

  # BigQuery JSON API style endpoints.
  if parts.len == 5 and parts[0] == "bigquery" and parts[1] == "v2" and parts[2] == "projects" and parts[4] == "datasets":
    let projectId = parts[3]
    if req.reqMethod == HttpGet:
      var datasets = newJArray()
      for _, dataset in state.bqDatasets:
        if dataset.projectId == projectId:
          datasets.add(bigQueryDatasetToJson(dataset))
      await jsonResponse(req, Http200, %*{"datasets": datasets})
      return

    if req.reqMethod == HttpPost:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return

      var datasetId = ""
      var datasetErr = ""
      if not parseBigQueryDatasetId(body, datasetId, datasetErr):
        await errorResponse(req, Http400, datasetErr)
        return

      var labels = initTable[string, string]()
      var labelsErr = ""
      if not parseBigQueryLabels(body, labels, labelsErr):
        await errorResponse(req, Http400, labelsErr)
        return

      var location = "US"
      if body.hasKey("location"):
        if body["location"].kind != JString:
          await errorResponse(req, Http400, "location must be string")
          return
        location = body["location"].getStr().strip()
        if location.len == 0:
          await errorResponse(req, Http400, "location must not be empty")
          return

      let datasetKey = bigQueryDatasetKey(projectId, datasetId)
      if state.bqDatasets.hasKey(datasetKey):
        await errorResponse(req, Http409, "Dataset already exists")
        return

      let ts = nowUnix()
      state.bqDatasets[datasetKey] = BigQueryDataset(
        projectId: projectId,
        datasetId: datasetId,
        location: location,
        labels: labels,
        tables: initTable[string, BigQueryTable](),
        createdAt: ts,
        updatedAt: ts
      )
      state.persistState()
      await jsonResponse(req, Http200, bigQueryDatasetToJson(state.bqDatasets[datasetKey]))
      return

  if parts.len == 6 and parts[0] == "bigquery" and parts[1] == "v2" and parts[2] == "projects" and parts[4] == "datasets":
    let projectId = parts[3]
    let datasetId = parts[5]
    let datasetKey = bigQueryDatasetKey(projectId, datasetId)
    if not state.bqDatasets.hasKey(datasetKey):
      await errorResponse(req, Http404, "Dataset not found")
      return
    let dataset = state.bqDatasets[datasetKey]

    if req.reqMethod == HttpGet:
      await jsonResponse(req, Http200, bigQueryDatasetToJson(dataset))
      return

    if req.reqMethod == HttpDelete:
      let queryParams = parseQueryParams(req.url.query)
      let deleteContents = queryParams.hasKey("deleteContents") and queryParams["deleteContents"].toLowerAscii() == "true"
      if dataset.tables.len > 0 and not deleteContents:
        await errorResponse(req, Http400, "Dataset is not empty (set deleteContents=true)")
        return
      state.bqDatasets.del(datasetKey)
      state.persistState()
      await jsonResponse(req, Http200, %*{})
      return

  if parts.len == 7 and parts[0] == "bigquery" and parts[1] == "v2" and parts[2] == "projects" and
      parts[4] == "datasets" and parts[6] == "tables":
    let projectId = parts[3]
    let datasetId = parts[5]
    let datasetKey = bigQueryDatasetKey(projectId, datasetId)
    if not state.bqDatasets.hasKey(datasetKey):
      await errorResponse(req, Http404, "Dataset not found")
      return
    let dataset = state.bqDatasets[datasetKey]

    if req.reqMethod == HttpGet:
      var tables = newJArray()
      for _, table in dataset.tables:
        tables.add(bigQueryTableToJson(table))
      await jsonResponse(req, Http200, %*{"tables": tables})
      return

    if req.reqMethod == HttpPost:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return

      var tableId = ""
      var tableErr = ""
      if not parseBigQueryTableId(body, tableId, tableErr):
        await errorResponse(req, Http400, tableErr)
        return

      var schema: seq[BigQueryField] = @[]
      var schemaErr = ""
      if not parseBigQuerySchema(body, schema, schemaErr):
        await errorResponse(req, Http400, schemaErr)
        return

      if dataset.tables.hasKey(tableId):
        await errorResponse(req, Http409, "Table already exists")
        return

      let ts = nowUnix()
      dataset.tables[tableId] = BigQueryTable(
        projectId: projectId,
        datasetId: datasetId,
        tableId: tableId,
        schema: schema,
        rows: @[],
        seenInsertIds: initTable[string, int64](),
        createdAt: ts,
        updatedAt: ts
      )
      dataset.updatedAt = ts
      state.persistState()
      await jsonResponse(req, Http200, bigQueryTableToJson(dataset.tables[tableId]))
      return

  if parts.len == 8 and parts[0] == "bigquery" and parts[1] == "v2" and parts[2] == "projects" and
      parts[4] == "datasets" and parts[6] == "tables":
    let projectId = parts[3]
    let datasetId = parts[5]
    let tableId = parts[7]
    let datasetKey = bigQueryDatasetKey(projectId, datasetId)
    if not state.bqDatasets.hasKey(datasetKey):
      await errorResponse(req, Http404, "Dataset not found")
      return
    let dataset = state.bqDatasets[datasetKey]
    if not dataset.tables.hasKey(tableId):
      await errorResponse(req, Http404, "Table not found")
      return

    if req.reqMethod == HttpGet:
      await jsonResponse(req, Http200, bigQueryTableToJson(dataset.tables[tableId]))
      return

    if req.reqMethod == HttpDelete:
      dataset.tables.del(tableId)
      dataset.updatedAt = nowUnix()
      state.persistState()
      await jsonResponse(req, Http200, %*{})
      return

  if parts.len == 9 and parts[0] == "bigquery" and parts[1] == "v2" and parts[2] == "projects" and
      parts[4] == "datasets" and parts[6] == "tables":
    let projectId = parts[3]
    let datasetId = parts[5]
    let tableId = parts[7]
    let action = parts[8]
    let datasetKey = bigQueryDatasetKey(projectId, datasetId)
    if not state.bqDatasets.hasKey(datasetKey):
      await errorResponse(req, Http404, "Dataset not found")
      return
    let dataset = state.bqDatasets[datasetKey]
    if not dataset.tables.hasKey(tableId):
      await errorResponse(req, Http404, "Table not found")
      return
    let table = dataset.tables[tableId]

    if action == "insertAll" and req.reqMethod == HttpPost:
      var body: JsonNode
      var parseErr = ""
      if not parseBodyJson(req, body, parseErr):
        await errorResponse(req, Http400, "Invalid JSON body: " & parseErr)
        return
      if not body.hasKey("rows") or body["rows"].kind != JArray:
        await errorResponse(req, Http400, "rows must be array")
        return

      var ignoreUnknownValues = false
      var boolErr = ""
      if not parseOptionalJsonBool(body, "ignoreUnknownValues", false, ignoreUnknownValues, boolErr):
        await errorResponse(req, Http400, boolErr)
        return
      var skipInvalidRows = false
      if not parseOptionalJsonBool(body, "skipInvalidRows", false, skipInvalidRows, boolErr):
        await errorResponse(req, Http400, boolErr)
        return

      let nowEpoch = nowUnix()
      table.pruneBigQueryInsertIds(nowEpoch)

      var candidates: seq[BigQueryRow] = @[]
      var insertErrors = newJArray()
      var index = 0
      for rowNode in body["rows"].items:
        var rowErr = ""
        var insertId = ""
        var normalizedRow = newJObject()
        var deduped = false

        if rowNode.kind != JObject:
          rowErr = "rows[" & $index & "] must be object"
        elif not rowNode.hasKey("json") or rowNode["json"].kind != JObject:
          rowErr = "rows[" & $index & "].json must be object"
        else:
          if rowNode.hasKey("insertId"):
            if rowNode["insertId"].kind != JString:
              rowErr = "rows[" & $index & "].insertId must be string"
            else:
              insertId = rowNode["insertId"].getStr().strip()

          if rowErr.len == 0 and insertId.len > 0 and table.seenInsertIds.hasKey(insertId):
            let insertedAt = table.seenInsertIds[insertId]
            if nowEpoch - insertedAt <= bigQueryInsertIdDedupWindowSeconds:
              deduped = true

          if rowErr.len == 0 and not deduped:
            if not validateBigQueryRow(table, rowNode["json"], ignoreUnknownValues, normalizedRow, rowErr):
              discard

        if rowErr.len > 0:
          insertErrors.add(%*{
            "index": index,
            "errors": [{
              "reason": "invalid",
              "message": rowErr
            }]
          })
        elif not deduped:
          candidates.add(BigQueryRow(insertId: insertId, data: normalizedRow, insertedAt: nowEpoch))
        inc index

      if insertErrors.len > 0 and not skipInvalidRows:
        await jsonResponse(req, Http200, %*{
          "kind": "bigquery#tableDataInsertAllResponse",
          "insertErrors": insertErrors
        })
        return

      for row in candidates:
        table.rows.add(row)
        if row.insertId.len > 0:
          table.seenInsertIds[row.insertId] = row.insertedAt

      if candidates.len > 0:
        table.updatedAt = nowEpoch
        dataset.updatedAt = nowEpoch
        state.persistState()

      var response = %*{
        "kind": "bigquery#tableDataInsertAllResponse"
      }
      if insertErrors.len > 0:
        response["insertErrors"] = insertErrors
      await jsonResponse(req, Http200, response)
      return

    if action == "data" and req.reqMethod == HttpGet:
      let queryParams = parseQueryParams(req.url.query)
      var maxResults = 100
      if queryParams.hasKey("maxResults"):
        try:
          maxResults = parseInt(queryParams["maxResults"])
        except ValueError:
          await errorResponse(req, Http400, "maxResults must be integer")
          return
      if maxResults < 1:
        maxResults = 1

      var startIndex = 0
      if queryParams.hasKey("pageToken"):
        try:
          startIndex = parseInt(queryParams["pageToken"])
        except ValueError:
          await errorResponse(req, Http400, "pageToken must be integer offset")
          return
      if startIndex < 0:
        startIndex = 0
      if startIndex > table.rows.len:
        startIndex = table.rows.len

      let endExclusive = min(table.rows.len, startIndex + maxResults)
      var rows = newJArray()
      for idx in startIndex ..< endExclusive:
        rows.add(bigQueryTableDataRow(table, table.rows[idx]))

      var response = %*{
        "totalRows": $table.rows.len,
        "rows": rows
      }
      if endExclusive < table.rows.len:
        response["pageToken"] = %($endExclusive)
      await jsonResponse(req, Http200, response)
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
      state.persistState()
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
      state.persistState()
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
      state.persistState()
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
          "topic": pubsubTopicFullName(projectId, sub.topic),
          "ackDeadlineSeconds": sub.ackDeadlineSeconds
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
      var ackDeadlineSeconds = 10
      var ackDeadlineErr = ""
      if not parseSubscriptionAckDeadline(body, ackDeadlineSeconds, ackDeadlineErr):
        await errorResponse(req, Http400, ackDeadlineErr)
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
      state.subscriptions[subName] = Subscription(
        name: subName,
        topic: topicName,
        ackDeadlineSeconds: ackDeadlineSeconds,
        queue: @[],
        inFlight: initTable[string, InFlightMessage]()
      )
      state.persistState()
      await jsonResponse(req, Http200, %*{
        "name": pubsubSubscriptionFullName(projectId, subName),
        "topic": pubsubTopicFullName(projectId, topicName),
        "ackDeadlineSeconds": ackDeadlineSeconds
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
      let pulled = state.pullMessages(subName, state.subscriptions[subName], maxMessages)
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
      var ackIds: seq[string] = @[]
      var ackErr = ""
      if not parseAckIds(body, ackIds, ackErr):
        await errorResponse(req, Http400, ackErr)
        return
      let ackedCount = acknowledgeMessages(state.subscriptions[subName], ackIds)
      if ackedCount > 0:
        state.persistState()
      await jsonResponse(req, Http200, %*{})
      return

    if req.reqMethod == HttpPost and parts[4].endsWith(":modifyAckDeadline"):
      let subName = parts[4][0 ..< parts[4].len - ":modifyAckDeadline".len]
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
      var ackIds: seq[string] = @[]
      var ackErr = ""
      if not parseAckIds(body, ackIds, ackErr):
        await errorResponse(req, Http400, ackErr)
        return
      var ackDeadlineSeconds = 0
      var deadlineErr = ""
      if not parseModifyAckDeadline(body, ackDeadlineSeconds, deadlineErr):
        await errorResponse(req, Http400, deadlineErr)
        return
      let changed = modifyAckDeadlines(state.subscriptions[subName], ackIds, ackDeadlineSeconds)
      if changed > 0:
        state.persistState()
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
      state.persistState()
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
      state.persistState()
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
      state.persistState()
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
          "queuedMessages": sub.queue.len,
          "inFlightMessages": sub.inFlight.len,
          "ackDeadlineSeconds": sub.ackDeadlineSeconds
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
      var ackDeadlineSeconds = 10
      var ackDeadlineErr = ""
      if not parseSubscriptionAckDeadline(body, ackDeadlineSeconds, ackDeadlineErr):
        await errorResponse(req, Http400, ackDeadlineErr)
        return

      let topicName = body["topic"].getStr()
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return
      if state.subscriptions.hasKey(subName):
        await errorResponse(req, Http409, "Subscription already exists")
        return
      state.subscriptions[subName] = Subscription(
        name: subName,
        topic: topicName,
        ackDeadlineSeconds: ackDeadlineSeconds,
        queue: @[],
        inFlight: initTable[string, InFlightMessage]()
      )
      state.persistState()
      await jsonResponse(req, Http201, %*{
        "name": subName,
        "topic": topicName,
        "ackDeadlineSeconds": ackDeadlineSeconds
      })
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
      let pulled = state.pullMessages(subName, state.subscriptions[subName], maxMessages)
      await jsonResponse(req, Http200, %*{"receivedMessages": pulled})
      return

  if parts.len == 5 and parts[0] == "pubsub" and parts[1] == "v1" and
      parts[2] == "subscriptions" and parts[4] == "acknowledge":
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
      var ackIds: seq[string] = @[]
      var ackErr = ""
      if not parseAckIds(body, ackIds, ackErr):
        await errorResponse(req, Http400, ackErr)
        return
      let ackedCount = acknowledgeMessages(state.subscriptions[subName], ackIds)
      if ackedCount > 0:
        state.persistState()
      await jsonResponse(req, Http200, %*{})
      return

  if parts.len == 5 and parts[0] == "pubsub" and parts[1] == "v1" and
      parts[2] == "subscriptions" and parts[4] == "modifyAckDeadline":
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
      var ackIds: seq[string] = @[]
      var ackErr = ""
      if not parseAckIds(body, ackIds, ackErr):
        await errorResponse(req, Http400, ackErr)
        return
      var ackDeadlineSeconds = 0
      var deadlineErr = ""
      if not parseModifyAckDeadline(body, ackDeadlineSeconds, deadlineErr):
        await errorResponse(req, Http400, deadlineErr)
        return
      let changed = modifyAckDeadlines(state.subscriptions[subName], ackIds, ackDeadlineSeconds)
      if changed > 0:
        state.persistState()
      await jsonResponse(req, Http200, %*{})
      return

  if parts.len == 3 and parts[0] == "scheduler" and parts[1] == "v1" and parts[2] == "jobs":
    if req.reqMethod == HttpGet:
      var jobs = newJArray()
      for _, job in state.jobs:
        var jobJson = %*{
          "name": job.name,
          "topic": job.topic,
          "payload": job.payload,
          "everySeconds": job.everySeconds,
          "nextRunEpoch": job.nextRunEpoch,
          "lastRunEpoch": job.lastRunEpoch
        }
        if job.cron.expression.len > 0:
          jobJson["cron"] = %job.cron.expression
        jobs.add(jobJson)
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

      let hasEverySeconds = body.hasKey("everySeconds")
      let hasCron = body.hasKey("cron")
      if hasEverySeconds == hasCron:
        await errorResponse(req, Http400, "Specify exactly one of everySeconds or cron")
        return

      let topicName = body["topic"].getStr()
      if not state.topics.hasKey(topicName):
        await errorResponse(req, Http404, "Topic not found")
        return

      var everySeconds = 0
      var cron = CronSchedule()
      if hasEverySeconds:
        if body["everySeconds"].kind != JInt:
          await errorResponse(req, Http400, "everySeconds must be integer")
          return
        everySeconds = body["everySeconds"].getInt()
        if everySeconds < 1:
          await errorResponse(req, Http400, "everySeconds must be >= 1")
          return
      else:
        if body["cron"].kind != JString:
          await errorResponse(req, Http400, "cron must be string")
          return
        let cronExpression = body["cron"].getStr().strip()
        if cronExpression.len == 0:
          await errorResponse(req, Http400, "cron must not be empty")
          return
        var cronErr = ""
        if not parseCronExpression(cronExpression, cron, cronErr):
          await errorResponse(req, Http400, "Invalid cron expression: " & cronErr)
          return

      var payload = ""
      if body.hasKey("payload") and body["payload"].kind == JString:
        payload = body["payload"].getStr()

      let ts = nowUnix()
      var nextRunEpoch = 0'i64
      if everySeconds > 0:
        nextRunEpoch = ts + int64(everySeconds)
      else:
        nextRunEpoch = nextCronRunEpoch(cron, ts)
        if nextRunEpoch == 0:
          await errorResponse(req, Http400, "Invalid cron expression: unable to find next run time within 366 days")
          return

      state.jobs[jobName] = SchedulerJob(
        name: jobName,
        topic: topicName,
        payload: payload,
        everySeconds: everySeconds,
        cron: cron,
        nextRunEpoch: nextRunEpoch,
        lastRunEpoch: 0
      )
      var response = %*{
        "name": jobName,
        "topic": topicName,
        "everySeconds": everySeconds,
        "nextRunEpoch": nextRunEpoch
      }
      if cron.expression.len > 0:
        response["cron"] = %cron.expression
      await jsonResponse(req, Http201, response)
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

