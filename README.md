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
  server/   entry point for the TCP server
  client/   entry point for the miner client
server/     TCP server, request routing, session handling, statistics
miner/      autonomous TCP client
pool/
  session/      connected client state
  dispatcher/   job generation and broadcast
protocol/   message parsing and serialization
infra/db/   PostgreSQL connection and bulk upsert of statistics
```

## Architecture

The system have 3 main components that communicate through well-defined boundaries.

### Server

Server represents the TCP server.

- It accepts TCP connections and spawns a goroutine per client (method `handleClient`).
- Each client goroutine runs a `handleSession` loop that reads with newline delimited ('\n') JSON messages and dispatches them through a middleware chain (`register` method from router).
- Routes are registered on `routeManager`, which registers them on router. You can pass the protocol method and the middleware (if any used).
- The `authorize` method is open.
- The `submit` method requires prior authentication via `authMiddleware`.
- The server holds a `sync.RWMutex` that protects its shared mutable state: the `clients` map, the `stats` map and the `listener`. Multiple goroutines access all 3 concurrently, client goroutines read and write `clients` and `stats` on every ,message. The dispatcher goroutine reads `clients` during broadcasts and the `WaitForAddr` (test helper) reads `listener during startup`.

### Session

Session represents a single connected client.

- It intentionally uses two mutexes.
    - The first, `mu` protects session state such as the username, authentication flag, used nonces, and last submission timestamp.
    - The second, `writeMu` serializes writes to the TCP connection.

    The separation matters because goroutines can need to write to the same connection at the same time, the `handleSession` goroutine may be sending a submit response while the dispatcher is broadcasting a new job to the same client. Without a dedicated write lock, those two concurrent writes would corrupt the TCP stream. Keeping them under separate locks means states reads and TCP writes never block each other unnecessarily.

### Dispatcher

Dispatcher runs a background ticker that fires every 30 seconds to dispatch jobs.

- On each tick it:
    - Generate a cryptographically random nonce using `crypto/rand`.
    - Increments the job ID.
    - Stores the job in a history map.
    - And sends the job to the server through a channel.

    Keeping the full current history map rather than only the current job means the server can validate submission against past jobs, which is required by the protocol. The server reads from this channel in `listenDispatcher` and calls `broadcastJob`, which snapshots the authenticated client list under a read lock and then writes to each client outside the lock to avoid holding it during potentially slow TCP writes.

### Miner

Miner represents a fully autonomous TCP client connection for the miner.

- It runs two concurrent goroutines after connecting:
    - `receiveJobs`: reads server messages in a loop and places incoming job broadcasts into a buffered channel of size one. If the channel is already full when a new job arrives, the old job is drained and replaced because the miner should always work on the most recent job.
    - `processJobs`: reads from the channel and calls `submit`, which generates a random client nonce, computes `SHA256(serverNonce + clientNonce)` and sends the result. A 1 minute ticker here ensures the miner resubmits the current job if no new job has arrived, satisfying the protocol requirement of at least one submission per minute.

## Protocol

All messages are delimited by newline using JSON over a persistent TCP connection.

- The client sends `authorize` once after connecting, and then `submit` for each job result.
- The server responds to both with `{"id": <same id>, "result": true}` on success or `{"id": <same id>, "result": false, "error": "<message>"}` on failure.
- The server broadcasts new jobs to all authenticated clients every 30 seconds using `"id": null, "method": "job", "params": {"job_id": N, "server_nonce": "<hex>"}}`.
- The SHA256 input is the concatenation of the `server_nonce` and `client_nonce` as plain strings. The order will matter: `SHA256("123" + "456")` is not `SHA256("456" + "123")`

Error messages are fixed strings defined by the protocol: "Task does not exist", "Invalid result", "Submission too frequent", and "Duplicate submission".

## Statistics

The server accumulates submission counts in memory per username and flushes them to PostgreSQL every minute. The flush uses a single bulk `INSERT ... ON CONFLICT DO UPDATE` that increments the existing count rather than replacing it. If the flush fails, the counts are merged back into the in-memory map so no submissions are lost before the next flush cycle.

## Design Decisions and Trade-offs

**Two mutexes in Session**: The alternative would be a single mutex protecting everything including writes. The cost of that approach is lock contention: a broadcast to a slow client would block any goroutine trying to read session state for that client. 

Since writes and state reads are truly independent operations, keeping separate locks gives better throughput under load.

**Atomic counter for session IDs**: An earlier version derived the session ID from `len(clients)`, and using the struct mutex to access. This was a bug: if a client connects, disconnects, and another connects, the two clients would share the same ID. An `atomic.Uint64` that only increments guarantees uniqueness for the lifetime of the process.

**Snapshot before broadcast**: `broadcastJob` takes a read lock, copies the list of authenticated clients into a local slice, releases the lock, and then writes to each client. This means a new client connecting during a broadcast does not block the broadcast, and a slow write to one client does not hold the lock while other goroutines are waiting.

**Stats re-enqueue on flush failure**: If `UpsertSubmissions` returns an error, the counts from the failed batch are merged back into `s.stats` so they are included in the next flush cycle. The alternative would be dropping them on error, which would silently undercount submissions.

**`net.Pipe` in tests**:. The miner and server integration tests use `net.Pipe` to create in-memory connections. This avoids real TCP overhead and port allocation, makes tests deterministic, and keeps them fast. The trade-off is that `net.Pipe` is synchronous, writes block until the other side reads, which requires careful use of goroutines in test setup. And that's why im using `go io.Copy(io.Discard, clientConn)` on some tests, which will essentially reads and discards everything that arrives, doesn't matter the content.