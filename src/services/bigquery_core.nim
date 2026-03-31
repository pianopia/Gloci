proc bigQueryDatasetKey(projectId: string; datasetId: string): string =
  projectId & ":" & datasetId

proc isValidBigQueryIdentifier(value: string): bool =
  if value.len < 1 or value.len > 1024:
    return false
  if not (value[0].isAlphaAscii() or value[0] == '_'):
    return false
  for ch in value:
    if not (ch.isAlphaNumeric() or ch == '_'):
      return false
  true

proc parseOptionalJsonBool(body: JsonNode; key: string; defaultValue: bool; value: var bool; err: var string): bool =
  value = defaultValue
  if not body.hasKey(key):
    return true
  if body[key].kind != JBool:
    err = key & " must be boolean"
    return false
  value = body[key].getBool()
  true

proc normalizeBigQueryFieldType(fieldType: string): string =
  let normalized = fieldType.strip().toUpperAscii()
  case normalized
  of "INT64":
    "INTEGER"
  of "BOOL":
    "BOOLEAN"
  of "FLOAT64":
    "FLOAT"
  else:
    normalized

proc isSupportedBigQueryFieldType(fieldType: string): bool =
  case fieldType
  of "STRING", "INTEGER", "FLOAT", "BOOLEAN", "TIMESTAMP", "DATE", "DATETIME", "TIME", "BYTES", "JSON":
    true
  else:
    false

proc bigQueryLabelsToJson(labels: Table[string, string]): JsonNode =
  result = newJObject()
  for key, value in labels:
    result[key] = %value

proc bigQueryFieldToJson(field: BigQueryField): JsonNode =
  %*{
    "name": field.name,
    "type": field.fieldType,
    "mode": field.mode
  }

proc bigQueryTableToJson(table: BigQueryTable): JsonNode =
  var fields = newJArray()
  for field in table.schema:
    fields.add(bigQueryFieldToJson(field))
  %*{
    "kind": "bigquery#table",
    "id": table.projectId & ":" & table.datasetId & "." & table.tableId,
    "tableReference": {
      "projectId": table.projectId,
      "datasetId": table.datasetId,
      "tableId": table.tableId
    },
    "schema": {"fields": fields},
    "numRows": $table.rows.len,
    "creationTime": $table.createdAt,
    "lastModifiedTime": $table.updatedAt
  }

proc bigQueryDatasetToJson(dataset: BigQueryDataset): JsonNode =
  %*{
    "kind": "bigquery#dataset",
    "id": dataset.projectId & ":" & dataset.datasetId,
    "datasetReference": {
      "projectId": dataset.projectId,
      "datasetId": dataset.datasetId
    },
    "location": dataset.location,
    "labels": bigQueryLabelsToJson(dataset.labels),
    "creationTime": $dataset.createdAt,
    "lastModifiedTime": $dataset.updatedAt
  }

proc parseBigQueryDatasetId(body: JsonNode; datasetId: var string; err: var string): bool =
  datasetId = ""
  if body.hasKey("datasetReference"):
    if body["datasetReference"].kind != JObject:
      err = "datasetReference must be object"
      return false
    if not body["datasetReference"].hasKey("datasetId") or body["datasetReference"]["datasetId"].kind != JString:
      err = "datasetReference.datasetId must be string"
      return false
    datasetId = body["datasetReference"]["datasetId"].getStr().strip()
  elif body.hasKey("datasetId") and body["datasetId"].kind == JString:
    datasetId = body["datasetId"].getStr().strip()
  else:
    err = "Request must include datasetReference.datasetId"
    return false

  if not isValidBigQueryIdentifier(datasetId):
    err = "Invalid datasetId"
    return false
  true

proc parseBigQueryTableId(body: JsonNode; tableId: var string; err: var string): bool =
  tableId = ""
  if body.hasKey("tableReference"):
    if body["tableReference"].kind != JObject:
      err = "tableReference must be object"
      return false
    if not body["tableReference"].hasKey("tableId") or body["tableReference"]["tableId"].kind != JString:
      err = "tableReference.tableId must be string"
      return false
    tableId = body["tableReference"]["tableId"].getStr().strip()
  elif body.hasKey("tableId") and body["tableId"].kind == JString:
    tableId = body["tableId"].getStr().strip()
  else:
    err = "Request must include tableReference.tableId"
    return false

  if not isValidBigQueryIdentifier(tableId):
    err = "Invalid tableId"
    return false
  true

proc parseBigQueryLabels(body: JsonNode; labels: var Table[string, string]; err: var string): bool =
  labels = initTable[string, string]()
  if not body.hasKey("labels"):
    return true
  if body["labels"].kind != JObject:
    err = "labels must be object"
    return false
  for key, value in body["labels"].pairs:
    if value.kind != JString:
      err = "labels values must be strings"
      return false
    labels[key] = value.getStr()
  true

proc parseBigQuerySchema(body: JsonNode; schema: var seq[BigQueryField]; err: var string): bool =
  schema = @[]
  var fieldsNode: JsonNode = nil
  if body.hasKey("schema"):
    if body["schema"].kind != JObject or not body["schema"].hasKey("fields") or body["schema"]["fields"].kind != JArray:
      err = "schema.fields must be array"
      return false
    fieldsNode = body["schema"]["fields"]
  elif body.hasKey("fields") and body["fields"].kind == JArray:
    fieldsNode = body["fields"]
  else:
    err = "Request must include schema.fields"
    return false

  if fieldsNode.len == 0:
    err = "schema.fields must not be empty"
    return false

  var seen = initTable[string, bool]()
  for fieldNode in fieldsNode.items:
    if fieldNode.kind != JObject:
      err = "schema.fields must contain objects"
      return false
    if not fieldNode.hasKey("name") or fieldNode["name"].kind != JString:
      err = "schema field must include name"
      return false
    if not fieldNode.hasKey("type") or fieldNode["type"].kind != JString:
      err = "schema field must include type"
      return false

    let fieldName = fieldNode["name"].getStr().strip()
    if not isValidBigQueryIdentifier(fieldName):
      err = "Invalid schema field name: " & fieldName
      return false
    if seen.hasKey(fieldName):
      err = "Duplicate schema field name: " & fieldName
      return false

    let fieldType = normalizeBigQueryFieldType(fieldNode["type"].getStr())
    if not isSupportedBigQueryFieldType(fieldType):
      err = "Unsupported schema field type: " & fieldType
      return false

    var mode = "NULLABLE"
    if fieldNode.hasKey("mode"):
      if fieldNode["mode"].kind != JString:
        err = "schema field mode must be string"
        return false
      mode = fieldNode["mode"].getStr().strip().toUpperAscii()
    if mode != "NULLABLE" and mode != "REQUIRED":
      err = "Unsupported schema field mode: " & mode
      return false

    schema.add(BigQueryField(name: fieldName, fieldType: fieldType, mode: mode))
    seen[fieldName] = true
  true

proc isBigQueryIntegerValue(value: JsonNode): bool =
  case value.kind
  of JInt:
    true
  of JString:
    try:
      discard parseBiggestInt(value.getStr())
      true
    except ValueError:
      false
  else:
    false

proc isBigQueryFloatValue(value: JsonNode): bool =
  case value.kind
  of JInt, JFloat:
    true
  of JString:
    try:
      discard parseFloat(value.getStr())
      true
    except ValueError:
      false
  else:
    false

proc isBigQueryBooleanValue(value: JsonNode): bool =
  case value.kind
  of JBool:
    true
  of JString:
    let lowered = value.getStr().toLowerAscii()
    lowered == "true" or lowered == "false"
  else:
    false

proc valueMatchesBigQueryType(value: JsonNode; fieldType: string): bool =
  case fieldType
  of "STRING", "BYTES", "TIMESTAMP", "DATE", "DATETIME", "TIME":
    value.kind == JString
  of "INTEGER":
    isBigQueryIntegerValue(value)
  of "FLOAT":
    isBigQueryFloatValue(value)
  of "BOOLEAN":
    isBigQueryBooleanValue(value)
  of "JSON":
    true
  else:
    false

proc validateBigQueryRow(table: BigQueryTable; rowData: JsonNode; ignoreUnknownValues: bool;
    normalizedRow: var JsonNode; err: var string): bool =
  if rowData.kind != JObject:
    err = "row json must be object"
    return false

  normalizedRow = newJObject()
  var knownFields = initTable[string, bool]()
  for field in table.schema:
    knownFields[field.name] = true
    if not rowData.hasKey(field.name):
      if field.mode == "REQUIRED":
        err = "Missing required field: " & field.name
        return false
      continue

    let value = rowData[field.name]
    if value.kind == JNull:
      if field.mode == "REQUIRED":
        err = "Required field cannot be null: " & field.name
        return false
      normalizedRow[field.name] = value
      continue

    if not valueMatchesBigQueryType(value, field.fieldType):
      err = "Field type mismatch for " & field.name & " (expected " & field.fieldType & ")"
      return false
    normalizedRow[field.name] = value

  if not ignoreUnknownValues:
    for key, _ in rowData.pairs:
      if not knownFields.hasKey(key):
        err = "Unknown field: " & key
        return false
  true

proc pruneBigQueryInsertIds(table: BigQueryTable; nowEpoch: int64) =
  var expired: seq[string] = @[]
  for insertId, insertedAt in table.seenInsertIds:
    if nowEpoch - insertedAt > bigQueryInsertIdDedupWindowSeconds:
      expired.add(insertId)
  for insertId in expired:
    table.seenInsertIds.del(insertId)

proc bigQueryTableDataRow(table: BigQueryTable; row: BigQueryRow): JsonNode =
  var fields = newJArray()
  for field in table.schema:
    var value = newJNull()
    if row.data.kind == JObject and row.data.hasKey(field.name):
      value = row.data[field.name]
    fields.add(%*{"v": value})
  %*{"f": fields}
