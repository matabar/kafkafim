# Captured Elasticsearch responses

Real `_bulk` response bodies from `docker.elastic.co/elasticsearch/elasticsearch:8.19.0`
(single node), captured on 2026-11-07 with the query string kafkafim sends,
`filter_path=errors,items.*.status,items.*.error`, unless noted. `ErrorClassifierTest`
and `BulkResponseTest` read them; the HTTP status of the request-level ones is in the
file name. Setup: index `kfm-orders` (`n: long`), index `kfm-strict` (`dynamic: strict`,
`n: long`), `action.auto_create_index: false`.

| File | HTTP | How it was produced |
|---|---|---|
| `bulk-all-ok.json` | 200 | `index` + `create` of new ids |
| `bulk-create-conflict-unfiltered.json` | 200 | the same body again **without** `filter_path`: `index` updates (200), `create` conflicts (409, document already exists) |
| `bulk-mixed-errors.json` | 200 | in order: ok; `n: "not-a-number"` (`document_parsing_exception`); unknown field on the strict index; `create` of an existing id; external version 3 below stored 5; external version 7 above 5; missing index (`index_not_found_exception`); unknown ingest pipeline (`illegal_argument_exception`); object for a `long` |
| `bulk-item-forbidden.json` | 200 | user with `write` on `kfm-orders` only, writing to `kfm-orders` and `kfm-strict` (item-level 403 `security_exception`) |
| `bulk-item-rejected.json` | 200 | `thread_pool.write.size: 1`, `queue_size: 1`, 30-shard index, 8 documents: item-level 429 `es_rejected_execution_exception` |
| `request-400-malformed.json` | 400 | body without the final line feed |
| `request-401.json` | 401 | wrong password |
| `request-403.json` | 403 | user with the `viewer` role only (rejected before items are evaluated) |
| `request-413.json` | 413 | `http.max_content_length: 2kb`, 40 documents (empty body) |
| `request-429.json` | 429 | `indexing_pressure.memory.limit: 10kb`, one 11 KB document (coordinating rejection) |
