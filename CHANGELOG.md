# Changelog

## 1.1.0 (2026-07-12)


### Features

* **compat:** guarantee Node and Go interoperability with BullMQ v4.12.2 at a01bb0b0345509cde6c74843323de6b67729f310 on Redis standalone and Cluster ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **events:** add typed QueueEvents lifecycle subscriptions ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **flows:** add parent-child flows and dependency inspection ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **queue:** add typed Queue, Worker, and Job APIs ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **repeat:** add BullMQ-compatible repeatable jobs ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **worker:** add manual processing, progress, retries, pause, and graceful shutdown ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))


### Bug Fixes

* **compat:** align Lua scripts, wire options, and Redis keys with BullMQ v4.12.2 ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
* **compat:** document the intentional BullMQ v4.12.2 deviations ([855a021](https://github.com/Codycody31/gobullmq/commit/855a021308b2c61a1bf05ebc949c35362d055ad4))
