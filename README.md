## Performance Evaluation

**Setup:** `tests/loadTest.js` simulates clients connecting over WebSocket to the server
on the same machine. Each client sends 50 direct messages (one every 50 ms) to the next
client in a ring. Latency is measured from just before send until the recipient receives
the message. Byte counts come from the server's `/metrics` endpoint. A warm-up run is
discarded, and the table shows one representative run.

| Protocol | Clients | Avg latency (ms) | p95 (ms) | Lost | Duplicates | Bytes in/msg | Bytes out/msg |
|---|---|---|---|---|---|---|---|
| JSON | 2  | 4.01  | 5.78  | 0 | 0 | 94.6 | 198.4 |
| JSON | 5  | 4.83  | 8.52  | 0 | 0 | 94.6 | 198.4 |
| JSON | 20 | 15.27 | 31.29 | 0 | 0 | 95.6 | 200.4 |
| Text | 2  | 3.89  | 5.71  | 0 | 0 | 55.6 | 117.4 |
| Text | 5  | 4.83  | 9.09  | 0 | 0 | 55.6 | 117.4 |
| Text | 20 | 14.93 | 28.01 | 0 | 0 | 56.6 | 119.4 |

**1. Latency:** Average end-to-end latency was about 4 ms with 2 and 5 clients and
about 15 ms with 20 clients on a local network.

**2. Protocol comparison:** The custom text protocol uses about 41% fewer bytes per
message than JSON in both directions (55.6 vs 94.6 bytes in, 117.4 vs 198.4 bytes out),
since it has no field names or quoting. Latency was nearly identical, so the benefit is
bandwidth, not speed. The cost is that text is harder to extend and relies on `|` as a
delimiter.

**3. Scalability:** With 2, 5 and 20 concurrent clients, delivered throughput scaled
from about 33 to 326 messages/s with zero lost and zero duplicated messages, thanks to
per-sender message IDs and the server's ack/dedup logic.