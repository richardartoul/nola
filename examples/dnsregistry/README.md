# Overview

This package demonstrates how to setup NOLA with actor modules written in Golang and using a DNS-backed registry. The example can be run directly from the root of the NOLA repository:

```bash
go run ./examples/dnsregistry/main.go
```

Note that the DNS registry should be safe for production use, however, it does not provide the same strict linearizability guarantees or built in KV storage that the FoundationDB backed registry does as modeled in the [stateright formal model](../../proofs/stateright/activation-cache). However, it is still suitable for use-cases that don't care about strict linearizability or durability. For example, as a coordination point for in-memory state, distributed synchronization primitives (ratelimiting), or for implementing smart/programmable caches.
