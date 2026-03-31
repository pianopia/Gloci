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

  InFlightMessage = object
    message: PubsubMessage
    deadlineEpoch: int64

  Subscription = ref object
    name: string
    topic: string
    ackDeadlineSeconds: int
    queue: seq[PubsubMessage]
    inFlight: Table[string, InFlightMessage]

  BigQueryField = object
    name: string
    fieldType: string
    mode: string

  BigQueryRow = object
    insertId: string
    data: JsonNode
    insertedAt: int64

  BigQueryTable = ref object
    projectId: string
    datasetId: string
    tableId: string
    schema: seq[BigQueryField]
    rows: seq[BigQueryRow]
    seenInsertIds: Table[string, int64]
    createdAt: int64
    updatedAt: int64

  BigQueryDataset = ref object
    projectId: string
    datasetId: string
    location: string
    labels: Table[string, string]
    tables: Table[string, BigQueryTable]
    createdAt: int64
    updatedAt: int64

  CronSchedule = object
    expression: string
    minutes: array[60, bool]
    hours: array[24, bool]
    dayOfMonth: array[32, bool]
    months: array[13, bool]
    dayOfWeek: array[7, bool]
    dayOfMonthWildcard: bool
    dayOfWeekWildcard: bool

  SchedulerJob = ref object
    name: string
    topic: string
    payload: string
    everySeconds: int
    cron: CronSchedule
    nextRunEpoch: int64
    lastRunEpoch: int64

  AppState = ref object
    buckets: Table[string, Bucket]
    topics: Table[string, Topic]
    subscriptions: Table[string, Subscription]
    jobs: Table[string, SchedulerJob]
    bqDatasets: Table[string, BigQueryDataset]
    nextMessageId: uint64
    nextAckId: uint64
    persistencePath: string

proc newAppState(persistencePath: string = ""): AppState =
  AppState(
    buckets: initTable[string, Bucket](),
    topics: initTable[string, Topic](),
    subscriptions: initTable[string, Subscription](),
    jobs: initTable[string, SchedulerJob](),
    bqDatasets: initTable[string, BigQueryDataset](),
    nextMessageId: 0'u64,
    nextAckId: 0'u64,
    persistencePath: persistencePath
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

const bigQueryInsertIdDedupWindowSeconds = 60'i64
