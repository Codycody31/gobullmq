// Package gobullmq is a BullMQ-compatible job queue for Go, backed by Redis.
//
// It speaks the exact Redis wire format of BullMQ v4.12.2 at upstream commit
// a01bb0b0345509cde6c74843323de6b67729f310: the Lua script layer is
// byte-identical to that upstream tag, and the data model (keys,
// job hashes, packed options, event streams) matches what a JS BullMQ
// v4.12.2 queue or worker reads and writes. Go and Node.js processes can
// therefore produce and consume jobs on the same queues. The few deliberate
// behavioral differences are listed under "Intentional deviations" in the
// README.
//
// The [compatibility matrix] ties each Go minor release line to one exact
// BullMQ version. After v1.1.0 is published, v1.1.0 selects that exact Go
// release and v1.1 selects the latest fix that retains the BullMQ v4.12.2
// baseline.
//
// The core types are generic over the job payload:
//
//   - Queue[D] adds jobs whose data is of type D (JSON-marshaled on the wire).
//   - Worker[D, R] processes jobs of data type D and produces results of
//     type R.
//   - Job[D] is the typed handle passed to processors and returned by
//     getters.
//
// Use any JSON-marshalable type for D and R; use a struct for structured
// payloads or map[string]any / any for schemaless ones.
//
// Redis clients are externally managed: constructors take a redis.Cmdable
// and never close it. Blocking consumers (a Worker's job fetch, a
// QueueEvents stream read) monopolize a connection and set their own CLIENT
// SETNAME, so give the Queue, each Worker, and each QueueEvents instance its
// own client rather than sharing one.
//
// [compatibility matrix]: https://github.com/Codycody31/gobullmq/blob/main/COMPATIBILITY.md
package gobullmq
