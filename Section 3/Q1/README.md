# Section 3 – Q1: Collaborative Document Editing using gRPC

## Files
| File | Contents |
|---|---|
| `document.proto` | The `DocumentService` definition and all its messages |
| `server.py` | Document Server: in-memory storage, editing, synchronization, update streaming |
| `client.py` | Client CLI: menu options 1–5 and typed commands; prints updates while you keep typing |
| `generate_proto.py` | Generates `document_pb2.py` / `document_pb2_grpc.py` from the proto file. `server.py` and `client.py` run it automatically when needed |
| `test_concurrent.sh` | Concurrent-edit test: runs the question's Step 5, then a stress run that checks for corruption |
| `requirements.txt` | `grpcio`, `grpcio-tools` |

## Setup
```bash
pip install --user -r requirements.txt     # or: python3 -m pip install --user grpcio grpcio-tools
```
The gRPC code is generated from `document.proto` the first time `server.py` or `client.py` runs, and again whenever the proto file changes. To generate it by hand:
```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto
```

## Run
```bash
python3 server.py [address]                  # default 0.0.0.0:50051
python3 client.py [server_address] [name]    # default localhost:50051; name defaults to host:pid
```
Client commands (type the number and answer the prompts, or type the command directly):
```
1. Create Document       create <name> "<content>"
2. Open Document         open <name>
3. Edit Document         edit <name> <position> "<text>"
4. Subscribe to Updates  subscribe <name>
5. Exit                  exit
```
`position` is a 0-based character offset, and `0 ≤ position ≤ length` of the document. Quotes group words, and `\"` inserts a literal quote.

### On the RCE cluster (server and clients on different nodes)
```bash
salloc --nodes=3 --ntasks-per-node=1          # on the login node
scontrol show hostnames $SLURM_JOB_NODELIST   # e.g. node01 node02 node03
```
Open three terminals. In each one, `ssh cs3401.46@rce.iiit.ac.in` and then:
```bash
# Terminal 1 – server
ssh node01
cd ~/Assignment3/"Section 3/Q1" && python3 server.py 0.0.0.0:50051

# Terminal 2 – Client 1
ssh node02
cd ~/Assignment3/"Section 3/Q1" && python3 client.py node01:50051 client1

# Terminal 3 – Client 2
ssh node03
cd ~/Assignment3/"Section 3/Q1" && python3 client.py node01:50051 client2
```
The server must listen on `0.0.0.0` so other nodes can reach it. `localhost` only accepts connections from the same node. When you are done, stop the server with Ctrl+C and run `exit` in the `salloc` shell to release the nodes.

## Design

### RPCs (`document.proto`)
| RPC | Type | Behaviour |
|---|---|---|
| `CreateDocument` | unary | Fails if the name is empty or already used |
| `GetDocument` | unary | Returns the content and the version (number of edits so far) |
| `EditDocument` | unary | Inserts `text` at `position`, returns the new content and version. Fails for an unknown document or an out-of-range position |
| `SubscribeToUpdates` | **server streaming** | The first message is the current snapshot (`snapshot = true`) and confirms that the subscription is registered. After that, one `DocumentUpdate` with the full new content is sent per edit, until the client cancels |

Errors in the unary RPCs are returned as `success = false` with a `message`, so the client can tell a rejected request (for example, a bad position) apart from a network failure (a gRPC error).

### Synchronization (`server.py`)
The server handles requests on a thread pool, so several RPCs run at the same time.
- **Document dictionary.** A `threading.Lock` protects the name → document dictionary. `CreateDocument` checks whether the name exists and inserts it under this lock, so two clients creating the same name cannot both succeed.
- **One lock per document.** It protects the content, the version and the subscriber list. `EditDocument` validates the position, builds the new string, increments the version and queues the update for every subscriber, all while holding this lock. An edit is a read-modify-write of the whole string. Without the lock, two concurrent edits could both read the old text and one of them would be lost, and Python's GIL does not prevent this. With the lock, concurrent edits to the same document are applied one at a time, in the order the server processes them. Edits to different documents don't block each other. The question does not require OT or CRDTs, so an edit's position refers to the document as it is when the edit is applied.
- **Per-subscriber queue.** Each `SubscribeToUpdates` call runs on its own server thread and yields updates from its own `queue.Queue`. An editor only adds an update to the queue, so a slow or stuck subscriber never delays edits. Every subscriber receives every version, in order.
- **Snapshot on subscribe.** Registration and queueing the snapshot happen under the document lock, so no edit can fall between the snapshot and the first update.
- **Lock order.** Locks are always taken in the order dictionary → document, and the dictionary lock is released before a document lock is taken. This rules out deadlock.
- **Disconnects and shutdown.** `context.add_callback` puts a stop marker in the queue when the client cancels or disconnects, or when the server stops. The stream then ends and the subscriber is removed from the document.
- **Thread pool size.** An open subscription occupies one worker thread, so `MAX_WORKERS = 64` limits how many subscriptions and in-flight requests can exist at once.

### Client (`client.py`)
Each `subscribe` starts a reader thread for that stream. The thread prints `[Update] …` whenever an update arrives and then redraws the prompt, while the main thread keeps reading commands. `subscribe` waits for the server's snapshot before printing `Subscribed to updates.`, so once the message appears, no later edit can be missed. On `exit`, the client cancels every stream and waits for the reader threads to finish.

## Demonstration
The transcripts below come from the program's real output. The server was on `localhost:50061`, and the two clients were started with the names `client1` and `client2`.

**1–2. Create a document; two clients open it**
```
client1> create report.txt "Hello World"
[Client] Document report.txt created.
client1> open report.txt
[Client] Hello World
client2> open report.txt
[Client] Hello World
```
**3. Client 2 subscribes. 4–5. Client 1 edits, and Client 2 receives the update automatically**
```
client2> subscribe report.txt
[Client] Subscribed to updates.
client1> edit report.txt 6 "Distributed "
[Client] Edit applied (version 1): Hello Distributed World

client2:
[Update] Document report.txt modified.
Hello Distributed World
```
Server log:
```
[Server] created report.txt (11 chars)
[Server] client2 subscribed to report.txt
[Server] report.txt v1: client1 inserted "Distributed " at 6 -> notified 1 subscriber(s)
[Server] client2 unsubscribed from report.txt
```
**6. Two clients edit concurrently.** `./test_concurrent.sh` starts a server and sends both edits at the same moment. It then runs a stress test: 2 clients send N edits each, as fast as they can, to the same document while a third client is subscribed.
```
=== Part 1: two clients edit at position 6 at the same time ===
[Client] Edit applied (version 1): Hello Systems World
[Client] Edit applied (version 2): Hello Distributed Systems World
  Final document: Hello Distributed Systems World
  PASS  both edits applied, nothing corrupted

=== Part 2: 200 edits from each of 2 clients, 1 subscriber ===
  400 edits in 179 ms (including client start-up)
  PASS  all 400 edits acknowledged
  PASS  length is 11 + 200x12 + 200x8 = 4011
  PASS  200 x "Distributed " and 200 x "Systems "
  PASS  removing the inserted words gives "Hello World"
  PASS  subscriber got all 400 updates, in order, ending with the final document

All checks passed.
```
The same checks pass with `./test_concurrent.sh 1000` (2000 edits). The two clients' edits are genuinely interleaved: in a separate run of 2 × 200 edits, consecutive words in the final document came from different clients 337 times out of 399. To run the test against a server on another RCE node, use `./test_concurrent.sh 200 node01:50051`.

## Error handling
| Input | Client output |
|---|---|
| `open nope.txt` | `Error: document nope.txt not found` |
| `edit a.txt 999 "x"` | `Error: position 999 is outside the document (length 10)` |
| `edit a.txt -1 "x"` | `Error: position must be a non-negative integer` |
| `create a.txt "dup"` (name exists) | `Error: document a.txt already exists` |
| `subscribe a.txt` twice | `Already subscribed to a.txt.` |
| `edit a.txt "unclosed` | `Error: No closing quotation` |
| server not running | `Error: cannot reach server (…)` |
