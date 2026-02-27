# TCP Mining Pool

A TCP based message processing system that simulates a simplified mining pool. The server maintains persistent connections with miners, distributes jobs every 30 seconds, validates SHA256 submissions, and aggregates statistics in PostgreSQL.

## Requirements

- Go 1.25+
- Docker and Docker Compose

## Running

Start the server (it also starts the PostgreSQL):

```bash
make execute
```

Connect a miner (run on as many terminals as you want):

```bash
make new-miner name=miner1
make new-miner name=miner2
```

To test manually with a raw TCP connection, use telnet or nc:

```bash
telnet localhost 12345
{"id":1,"method":"authorize","params":{"username":"miner1"}}
{"id":2,"method":"submit","params":{"job_id":1,"client_nonce":"abc","result":"<sha256>"}}
```

To stop everything, clean up binaries and Docker volumes:

```bash
make clean
```

## Running Tests

```bash
make test
```

This runs the full test suite with the race detector enabled. The tests do not require a database or Docker, they use in-memory connections using ```net.Pipe``` and fake dispatcher implementations.

## Project Structure

```bash
cmd/
  server/       entry point for the TCP server
  client/       entry point for the miner client
server/         TCP server, request routing, session handling, statistics
miner/          autonomous TCP client
pool/
  session/      connected client state
  dispatcher/   job generation and broadcast
protocol/       protocol layer, message parsing, serialization by method and errors
infra/
  db/           PostgreSQL connection and bulk upsert of statistics
  events/       RabbitMQ publisher and consumer for async submission persistence
```

## Protocol

All messages are delimited by newline using JSON over a persistent TCP connection. The `protocol` package owns all parsing and serialization, no other package calls `json.Unmarshal` directly or raw messages.

Every client message has 3 fields: `id`, `method` and `params`:

- The `id` correlates requests to responses.
- The `method` will determine how `params` is parsed
- The package uses a two-phase unmarshal where the outer message is parsed first to extract the method, and then `params` is unmarshaled into the correct typed struct (`AuthParams`, `SubmitParams` or `JobParams`) based on that method.
- Unknown methods are rejected at the parse layer before reaching any handler.

Error messages are defined as sentinel errors at the `protocol` layer, not in the server. This is intentional because these strings are part of the protocol contract, not server or client implementation details. The four protocouls errors are: `"Task does not exist"`, `"Invalid result"`, `"Submission too frequent"`, and `"Duplicate submission"`.

Workflow:

- The client sends `authorize` once after connecting, and then `submit` for each job result.
- The server always responds with `{"id": <same id>, "result": true}` on success or `{"id": <same id>, "result": false, "error": "<message>"}` on failure.
- The server broadcasts new jobs to all authenticated clients every 30 seconds using `"id": null, "method": "job", "params": {"job_id": N, "server_nonce": "<hex>"}}`.
- The SHA256 input is the concatenation of the `server_nonce` and `client_nonce` as plain strings. The order will matter: `SHA256("123" + "456")` is not `SHA256("456" + "123")`

## Design Decisions and Trade-offs

**Two mutexes in Session**: The alternative would be a single mutex protecting everything including writes. The cost of that approach is lock contention: a broadcast to a slow client would block any goroutine trying to read session state for that client. 

Since writes and state reads are truly independent operations, keeping separate locks gives better throughput under load.

**Atomic counter for session IDs**: An earlier version derived the session ID from `len(clients)`, and using the struct mutex to access. This was a bug: if a client connects, disconnects, and another connects, the two clients would share the same ID. An `atomic.Uint64` that only increments guarantees uniqueness for the lifetime of the process.

**Snapshot before broadcast**: `broadcastJob` takes a read lock, copies the list of authenticated clients into a local slice, releases the lock, and then writes to each client. This means a new client connecting during a broadcast does not block the broadcast, and a slow write to one client does not hold the lock while other goroutines are waiting.

**Stats re-enqueue on flush failure**: If `UpsertSubmissions` returns an error, the counts from the failed batch are merged back into `s.stats` so they are included in the next flush cycle. The alternative would be dropping them on error, which would silently undercount submissions.

**`net.Pipe` in tests**:. The miner and server integration tests use `net.Pipe` to create in-memory connections. This avoids real TCP overhead and port allocation, makes tests deterministic, and keeps them fast. The trade-off is that `net.Pipe` is synchronous, writes block until the other side reads, which requires careful use of goroutines in test setup. And that's why im using `go io.Copy(io.Discard, clientConn)` on some tests, which will essentially reads and discards everything that arrives, doesn't matter the content.

## Statistics

The server accumulates submission counts in memory per username and flushes them to PostgreSQL every minute. The flush uses a single bulk `INSERT ... ON CONFLICT DO UPDATE` that increments the existing count rather than replacing it. If the flush fails, the counts are merged back into the in-memory map so no submissions are lost before the next flush cycle.

## Message Queue

### How it replaces in memory flush

Without RabbitMQ the server accumulates submission counts in a `map[string]int` and fluxes to PostgreSQL every minute. The problem is that a server restart within window loses up to one minute of data. With RabbitMQ, each submission is durably enqueued immediately. If the server restarts before the consumer processes a message, RabbitMQ holds it until the consumer reconnects and process it, because messages are published with `DeliveryMode: Persistent` and the queue is declares ad durable.

### Design decision and trade-offs

**Buffered channel between handler and broker**: The publisher holds an internal Go channel of 1000 events. `handleSubmit` handler writes to this channel and returns immediately without waiting for the broker. A background goroutine drains the channel and sends to RabbitMQ. This means a slow or temporarily unavailable broker never delays the TCP response to the miner. If the buffer fills up, the event is dropped and logged. We prefer losing a single event over blocking the server's critical path.

**RabbitMQ is optional**: If the broker is unavailable at startup, the server logs a warning and continues running without async publishing. The critical path of accept authenticate, and submit does not depend on RabbitMQ being healthy. This ia a deliberate trade-off, because a mining pool shoul keep accepting work even if its statistics pipeline is degraded.

**Manual acknowledgement**: The consumer uses `auto-ack: false`, meaning a message is only removed from the queue after `msg.Ack` is called following successful database write. If the database is temporarily unavailable, the consumer calls `msg.Nack(false, true)` to requeue the message for a lter retry. If the message itself is malformed and will never parse correctly, it calls `msg.Nack(false, false)` to discard it permanently rather than retrying forever.