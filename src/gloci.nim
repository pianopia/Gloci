import std/[asynchttpserver, asyncdispatch, strutils, tables, json, os, times, uri]

include services/types_and_utils
include services/bigquery_core
include services/cron_core
include services/pubsub_core
include services/persistence
include services/runtime_ops
include services/router

proc main() =
  let persistencePath = getEnv("GLOCI_DATA_FILE", "").strip()
  let state = newAppState(persistencePath)
  state.loadState()
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
