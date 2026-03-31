proc parseCronFieldValue(value: string; minValue: int; maxValue: int; fieldName: string;
    parsed: var int; err: var string): bool =
  try:
    parsed = parseInt(value)
  except ValueError:
    err = "Invalid integer value in " & fieldName & " field: " & value
    return false
  if parsed < minValue or parsed > maxValue:
    err = fieldName & " value out of range (" & $minValue & "-" & $maxValue & "): " & $parsed
    return false
  true

proc parseCronField(field: string; minValue: int; maxValue: int; fieldName: string;
    values: var openArray[bool]; wildcard: var bool; err: var string): bool =
  for i in 0 ..< values.len:
    values[i] = false

  let trimmedField = field.strip()
  if trimmedField.len == 0:
    err = fieldName & " field must not be empty"
    return false

  wildcard = trimmedField == "*"
  for rawToken in trimmedField.split(","):
    let token = rawToken.strip()
    if token.len == 0:
      err = "Invalid empty token in " & fieldName & " field"
      return false

    var base = token
    var step = 1
    let slashPos = token.find('/')
    if slashPos >= 0:
      if slashPos == 0 or slashPos == token.high:
        err = "Invalid step token in " & fieldName & " field: " & token
        return false
      base = token[0 ..< slashPos].strip()
      let stepPart = token[slashPos + 1 .. ^1].strip()
      if stepPart.len == 0 or stepPart.contains("/"):
        err = "Invalid step token in " & fieldName & " field: " & token
        return false
      try:
        step = parseInt(stepPart)
      except ValueError:
        err = "Invalid step value in " & fieldName & " field: " & stepPart
        return false
      if step < 1:
        err = "Step value must be >= 1 in " & fieldName & " field"
        return false

    if base.len == 0:
      err = "Invalid token in " & fieldName & " field: " & token
      return false

    var rangeStart = minValue
    var rangeEnd = maxValue
    if base == "*":
      discard
    else:
      let dashPos = base.find('-')
      if dashPos >= 0:
        if dashPos == 0 or dashPos == base.high:
          err = "Invalid range token in " & fieldName & " field: " & token
          return false
        let startPart = base[0 ..< dashPos].strip()
        let endPart = base[dashPos + 1 .. ^1].strip()
        if startPart.len == 0 or endPart.len == 0:
          err = "Invalid range token in " & fieldName & " field: " & token
          return false
        if not parseCronFieldValue(startPart, minValue, maxValue, fieldName, rangeStart, err):
          return false
        if not parseCronFieldValue(endPart, minValue, maxValue, fieldName, rangeEnd, err):
          return false
        if rangeStart > rangeEnd:
          err = "Range start must be <= range end in " & fieldName & " field: " & token
          return false
      else:
        var singleValue = 0
        if not parseCronFieldValue(base, minValue, maxValue, fieldName, singleValue, err):
          return false
        rangeStart = singleValue
        if slashPos >= 0:
          rangeEnd = maxValue
        else:
          rangeEnd = singleValue

    var matchedToken = false
    var value = rangeStart
    while value <= rangeEnd:
      if ((value - rangeStart) mod step) == 0:
        if value >= low(values) and value <= high(values):
          values[value] = true
          matchedToken = true
      inc value
    if not matchedToken:
      err = "Token does not match any value in " & fieldName & " field: " & token
      return false

  for value in minValue .. maxValue:
    if value >= low(values) and value <= high(values) and values[value]:
      return true
  err = "No values selected for " & fieldName & " field"
  false

proc parseCronExpression(expression: string; cron: var CronSchedule; err: var string): bool =
  let fields = expression.splitWhitespace()
  if fields.len != 5:
    err = "cron must have 5 fields: minute hour day-of-month month day-of-week"
    return false

  cron = CronSchedule()
  cron.expression = fields.join(" ")

  var ignoreWildcard = false
  if not parseCronField(fields[0], 0, 59, "minute", cron.minutes, ignoreWildcard, err):
    return false
  if not parseCronField(fields[1], 0, 23, "hour", cron.hours, ignoreWildcard, err):
    return false
  if not parseCronField(fields[2], 1, 31, "day-of-month", cron.dayOfMonth, cron.dayOfMonthWildcard, err):
    return false
  if not parseCronField(fields[3], 1, 12, "month", cron.months, ignoreWildcard, err):
    return false
  if not parseCronField(fields[4], 0, 6, "day-of-week", cron.dayOfWeek, cron.dayOfWeekWildcard, err):
    return false
  true

proc floorToMinuteEpoch(epoch: int64): int64 =
  epoch - (epoch mod 60'i64)

proc cronWeekday(weekday: WeekDay): int =
  (ord(weekday) + 1) mod 7

proc cronMatches(cron: CronSchedule; epoch: int64): bool =
  let dt = fromUnix(epoch).utc
  if not cron.minutes[dt.minute]:
    return false
  if not cron.hours[dt.hour]:
    return false

  let monthValue = ord(dt.month)
  if monthValue < 1 or monthValue > 12 or not cron.months[monthValue]:
    return false

  let dayOfMonthValue = dt.monthday
  let dayOfWeekValue = cronWeekday(dt.weekday)
  let dayOfMonthMatch = dayOfMonthValue >= 1 and dayOfMonthValue <= 31 and cron.dayOfMonth[dayOfMonthValue]
  let dayOfWeekMatch = dayOfWeekValue >= 0 and dayOfWeekValue <= 6 and cron.dayOfWeek[dayOfWeekValue]

  if cron.dayOfMonthWildcard and cron.dayOfWeekWildcard:
    return true
  if cron.dayOfMonthWildcard:
    return dayOfWeekMatch
  if cron.dayOfWeekWildcard:
    return dayOfMonthMatch
  dayOfMonthMatch or dayOfWeekMatch

proc nextCronRunEpoch(cron: CronSchedule; fromEpoch: int64): int64 =
  var candidate = floorToMinuteEpoch(fromEpoch) + 60'i64
  let maxChecks = 366 * 24 * 60
  for _ in 0 ..< maxChecks:
    if cronMatches(cron, candidate):
      return candidate
    candidate += 60'i64
  0'i64
