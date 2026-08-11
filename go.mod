module go.codycody31.dev/gobullmq

go 1.24

retract [v1.0.0, v1.0.3] // Historical tags declare github.com/hellosekai/bull-golang and are not releases of this module.

require (
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.22.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
