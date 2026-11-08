# ADR-0030: Request-level retry logs name Elasticsearch's error types, not the response body

Status: accepted · Date: 2026-11-08 · Refines [ADR-0026](0026-bulk-retry-loop-in-the-sink.md)

## Context
A bulk request that fails as a whole (429, 5xx, 401/403, an unexpected status, an
unparsable 2xx body) is retried forever (ADR-0026, DESIGN §7). DESIGN §7 said such a
request is "logged with the body", and the sink logged the first 1024 characters of
Elasticsearch's response at WARN on every attempt.

Elasticsearch's `error.reason` can quote the document's field values (for example
"Preview of field's value: '…'" in a parsing error, or a `number_format_exception`'s
input string). Request-level retries repeat until someone intervenes, so one bad answer
would put those values into the logs once per backoff interval. This contradicts DESIGN
§10 ("Document bodies are never logged by default"). Item errors were already kept out of
the log. Their reason travels in the DLQ record, which holds the document anyway. A
request-level failure has no DLQ record to carry its reason.

The operator still needs something to act on: a stalled pipeline with only a status code
is hard to diagnose.

Options considered:

1. Status and error type at WARN; the body only at DEBUG.
2. Status and error type only; never the body.
3. Type plus reason with the values stripped out.
4. The full body at WARN, as before.

Option 3 was rejected because Elasticsearch quotes values in several formats (`[...]`,
`'...'`, escaped JSON strings), so stripping them by pattern is unreliable and gives false
confidence. Option 2 leaves a stalled pipeline nothing to debug from without a code
change. Option 4 is the leak.

## Decision
- The WARN line of a request-level retry names the status and Elasticsearch's
  `error.type` plus the distinct `error.root_cause[].type`s (at most 5). It never includes
  `error.reason` or any other text from the body. A body that is not JSON is logged as
  `non-JSON, <n> bytes` (a top-level array or scalar, or JSON nested too deep to parse,
  counts as not JSON); an empty body as `empty body`; JSON without an `error` object, by
  its size only. A 2xx body that is not a bulk response is logged by what was wrong with
  it (for example "bulk response has no items"), never by its content.
- A 401/403 keeps the security error's `reason`. It names the user, roles, action and
  index, which an operator needs to fix the grant, and no document data. Response headers
  echoed in the body (`WWW-Authenticate`) are dropped.
- The response body itself, cut to 1024 characters, is logged at DEBUG on the
  `io.github.matabar.kafkafim.sink.es.BulkSink` logger, one line per failed attempt. It is
  off by default; an operator enables it for that one logger while diagnosing.
- `SinkListener.onAuthFailure` carries the same summary (type, root-cause types,
  reason), never the raw body, for request-level and item-level auth failures.
  `SinkListener.onRequestRetry` carries a throwable only for a transport error; a
  malformed 2xx passes none, because the JSON parser's message can quote the body.
- The same holds for what leaves the process in telemetry: a request's
  `BatchReport.Request.error`, which becomes the `es.bulk` span's exception message
  (ADR-0029), is the transport exception or the fixed text "malformed response", never
  response text.

## Consequences
Logs at the default level hold no Elasticsearch text that can quote documents. Diagnosing
an unusual whole-request failure may take one extra step: raising the `BulkSink` logger
to DEBUG. The DEBUG body can still hold field values, so enabling it is a deliberate,
temporary operator action. The summary is built by `sink/es/ErrorSummary` and unit-tested
against captured Elasticsearch 8.19 responses.
