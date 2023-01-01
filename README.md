# Overview

NOLA is a distributed virtual actor system that is heavily inspired by [Cloudflare Durable Objects](https://developers.cloudflare.com/workers/learning/using-durable-objects/) and other virtual actor systems like [Orleans](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf).

Currently it is an experimental POC, however, the goal is to develop it into a production grade system that could be deployed to internal environments and used as a powerful primitive for building distributed applications.

NOLA leverages [WASM/WASI](https://webassembly.org/) as the fundamental building block for creating and executing virtual actors in a distributed environment. Currently, NOLA uses the excellent [wazero](https://wazero.io/) library for WASM compilation/execution, although additional runtimes may be added in the future. Actors can be written in any language that can be compiled to WASM. Communication with actors happens via RPC, so WASM modules must implement the [WAPC protocol](https://wapc.io/). Implementing this protocol is straightforward, and libraries already exist in a variety of languages. For example, writing a WAPC-compatible actor in Go is just a few lines of code with the [wapc-go](https://github.com/wapc/wapc-go) library:

```golang
package main

import (
	wapc "github.com/wapc/wapc-guest-tinygo"
)

func main() {
	wapc.RegisterFunctions(wapc.Functions{
		"echo":       echo,
	})
}

func echo(payload []byte) ([]byte, error) {
	wapc.HostCall("wapc", "testing", "echo", payload)
	return payload, nil
}
```

The program above can be compiled with [tinygo](https://tinygo.org/) and uploaded directly to NOLA's module registry. Once that is done, an arbitrary number of actors can be instantiated from that module.

The current implementation is a basic prototype, however, NOLA seeks to provide the following functionality / capabilities:

1. Actors can be instantiated on-demand and live forever (or until they're manually removed).
2. Communication with actors happens via RPC.
3. Actor execution is single-threaded and all RPCs/Invocations execute atomically.
4. Actors can spawn new actors and send messages to other actors.
5. Every actor comes with its own built-in durable KV storage.
6. The system is externally consistent / linerizable / strongly consistent in the same way that Cloudflare durable objects are. TODO: Expand on this.
7. Actors are "cheap". Millions of them can be created, and they can be evicted from memory when they're inactive (not actively receiving RPCs or doing useful work). An inactive actor will be "activated" on-demand as soon as someone issues an RPC for it.
8. By default, an Actor will only ever have a single live activation in the system at any given moment. In effect, every Actor is an HA singleton that NOLA ensures is always available. Inactive actors are automatically GC'd by the system until they become active again.
9. The system self heals by automatically detecting failed servers and removing them from the cluster. Actors on the failed server are automatically reactived on a healthy server on their next invocation/RPC.
10. An intelligent control plane that assigns individual actors to servers based relevant criteria like load, memory usage, and locality of communication.
11. Orleans-style scheduling such that activated actors can take actions on a regular basis. In addition, NOLA will support "scheduled RPCs" such that actors can be automatically activated to perform some operation on a regular basis.

## Key Technologies

1. [WASM/WASI](https://webassembly.org/)
2. [wazero](https://wazero.io/)
3. [WAPC protocol](https://wapc.io/)
4. [tinygo](https://tinygo.org/)
5. [FoundationDB](https://www.foundationdb.org/)

## Playground

The NOLA repository contains a simple playground for local experimentation. In the root of the directory run:

```bash
make server
```

to start a local instance of the NOLA server backed by an in-memory registry.

Next, run:

```bash
make run-playground
```

which will execute `./scripts/playground/basic.sh`. This will register a module in the `playground` namespace, instantiate a single actor named: `test_utils_actor_1`, and then invoke the `inc` function on the actor a few times and print the result.

## Implemented Functionality

NOLA is currently an MVP prototype with limited functionality. The current capabilities of the system are best understood via `environment_test.go`. It has many severe limitations currently:

1. The only Registry implementation is an in-memory fake (`registry/local.go`) that is lost on process restart.
2. The KV interface exposed to actors is not transactional.
3. Much more (see the TODOs section)

That said, NOLA is none the less a useful prototype with a basic foundation that can be easily expanded upon. Currently, it supports:

1. Uploading arbitrary WASM modules to the registry.
2. Once a module has been uploaded to the registry, any number of actors can be created based on that WASM module.
3. Once an actor has been created, any function that the actor exposes via WAPC can be invoked.
4. If the actor is not currently activated, then the WASM module will be fetched and compile on demand, and then an instance of the actor will be activated in memory.
5. Actor implementations can leverage durable KV storage (backed by the Registry itself) to persist data.
6. Actors can create new actors from their own module, or a completely different module.
7. Actors can communicate with other actors by invoking functions / operations on remote actors.

Actor's are not inherently replicated, and in general an actor will only ever be activated on a single server at any given time. However, the existing implementation of NOLA is already "highly available. NOLA has a service discovery mechanism where every "server" is constantly heartbeating the registry. If a server fails to heartbeat the registry for too long (currently hardcoded at 5s) then the registry will consider that server "dead" and all future invocations for actors that were previously activated on the failed server will trigger an on-demand reactivation on one of the remaining live servers. This means that NOLA will automatically self-heal and actors running on servers that fail will automatically be reactivated on a new server (assuming one is available). However, relocation does take time and the actor will be temporarily unavailable when a server fails. The window of unavailability for an actor is ~ proportional to the heatbeat TTL (currently 5 seconds) + the time it takes to download and recompile a WASM module on a new server.

The service discovery / heartbeating mechanism is also leveraged by the Registry to try and ensure that actor activations are evenly balanced across all the available servers. Everytime a server heartbeats the registry, it provides a view of its current state and load. The registry takes this view into account for future invocations when EnsureActivation() is called. Currently the only input to the load-balancing system is the number of actors currently activated on a given server. However, this will be expanded in the future to take CPU and memory utilization into account to deal with "hotspots" created by CPU and memory intensive actors.

`testdata/tinygo/util/main.go` demonstrates a simple utility actor (including KV storage and the ability to fork itself to create new actors) that is used heavily throughout the test suite.

See the "Playground" section of the README for instructions on how to run the playground to play around with NOLA locally.

## Roadmap

1. Write an FDB or Tigris backed registry implementation.
2. Fix the KV interface to be transactional.
3. See #TODOs.

## Benchmarks

There is a very simple set of single-threaded benchmarks in `benchmarks_test.go`. These benchmarks use a fake in-memory registry and a tiny WASM module with a function that does nothing but increment an in-memory counter. While these benchmarks are not representative of any realistic workloads, they're useful for understanding the maximum throughput which the basic NOLA framework could ever achieve in its current state.

On my 2021 M1 Max:

```
goos: darwin
goarch: arm64
pkg: github.com/richardartoul/nola/virtual
BenchmarkInvoke-10                       	  970826	      1209 ns/op	    827078 ops/s	     348 B/op	       8 allocs/op
BenchmarkCreateThenInvokeActor-10        	   15018	     74805 ns/op	     13368 ops/s	  187778 B/op	     531 allocs/op
BenchmarkActorToActorCommunication-10    	  251772	      4629 ns/op	    216048 ops/s	    1130 B/op	      28 allocs/op
PASS
ok  	github.com/richardartoul/nola/virtual	7.149s
```

In summary, if we ignore RPC and Registry overhead then NOLA is able to achieve 827k function calls/s on a single actor, instantiate new actors into memory and invoke a function on them at a rate of 13k/s, and support actors communicating with each other at a rate of 215k function calls/s. All of this is accomplished in a single-threaded manner on a single core.

Of course a production system will never achieve these results on a single core once a distributed registry and inter-server RPCs are being used. However, these numbers indicate that the most experimental aspect of NOLA's design (creating virtual actors by compiling WASM programs to Go assembly using the Wazero library and executing them on the fly) are efficient enough to handle large-scale workloads. Efficient Registry and RPC implementations will have to be built, but those are problems we already know how to solve.

# TODOs

1. Transactional KV.
2. FDB registry implementation.
3. ACL policy for module capabilities + access to host functions.
4. Limiting actor memory usage.
5. Scheduling.
6. WASM modules need dedicated storage so they can be large.
7. Cycle/dead-lock detection for inter-actor RPC.
8. Use a queue for buffering incoming actor RPCs + backpressure.

So many more things.