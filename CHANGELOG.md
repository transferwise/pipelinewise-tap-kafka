4.0.0 (2020-08-27)
-------------------

- Improve the performance of persisting kafka messages if the local store cannot perform frequent file appends and when it's causing high I/O issues   
- Switching from `jsonpath-ng` to `dpath` python library to improve the performance of extracting primary keys
- Change the syntax of `primary_keys` from JSONPath to `/slashed/paths` ala XPath

3.1.0 (2020-04-20)
-------------------

- Add `max_poll_records` option

3.0.0 (2020-04-03)
-------------------

- Add local storage of consumed messages and instant commit kafka offsets
- Add more configurable options: `consumer_timeout_ms`, `session_timeout_ms`, `heartbeat_interval_ms`, `max_poll_interval_ms`
- Add two new fixed output columns: `MESSAGE_PARTITION` and `MESSAGE_OFFSET`

2.1.1 (2020-03-23)
-------------------

- Commit offset from state file and not from the consumed messages

2.1.0 (2020-02-18)
-------------------

- Make logging customisable

2.0.0 (2020-01-07)
-------------------

- Rewamp the output schema with no JSON flattening

1.0.2 (2019-11-25)
-------------------

- Add 'encoding' as a configurable parameter

1.0.1 (2019-08-16)
-------------------

- Add license classifier

1.0.0 (2019-06-03)
-------------------

- Initial release
