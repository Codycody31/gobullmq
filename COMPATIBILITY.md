# Compatibility

Go release lines are tied to one exact BullMQ baseline. Pin the Go minor
version to receive fixes without moving to a different BullMQ wire format.

| Go selector | BullMQ | Upstream commit | Redis tested | Release branch | Status |
|---|---|---|---|---|---|
| `v1.1.x` | `4.12.2` | [`a01bb0b`](https://github.com/taskforcesh/bullmq/commit/a01bb0b0345509cde6c74843323de6b67729f310) | `7.4` standalone and cluster | `release/v1.1-bullmq-v4.12.2` | Release candidate |

The compatibility claim covers BullMQ `4.12.2` exactly. It does not imply a
`4.12.x` range. A different BullMQ baseline receives a different Go release
line and release branch.

After `v1.1.0` is published, select it with either:

```bash
go get go.codycody31.dev/gobullmq@v1.1.0  # exact release
go get go.codycody31.dev/gobullmq@v1.1    # latest fix on this BullMQ line
```

Until then, testers can fetch the current branch directly:

```bash
GOPROXY=direct go get go.codycody31.dev/gobullmq@main
```

Published tags are immutable. A bad release is corrected with a higher patch
and, when needed, a Go `retract` directive.
