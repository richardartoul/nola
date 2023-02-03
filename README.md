# Overview

NOLA is a distributed virtual actor system that is heavily inspired by [Cloudflare Durable Objects](https://developers.cloudflare.com/workers/learning/using-durable-objects/) and other virtual actor systems like [Orleans](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf). I highly recommend reading the Orleans paper if you're unfamiliar, but I will do my best to summarize.

If you're not familiar with virtual actor systems or Cloudflare durable objects, you can think of [Actors](https://en.wikipedia.org/wiki/Actor_model) as "classes" (state + logic) that come bundled with a single-threaded (per-actor) execution environment. This is a common pattern used in many applications for building large scale distributed systems, or even managing concurrency within a single process. It is employed by popular libraries / frameworks like [Erlang](https://www.erlang.org/), [Akka](https://akka.io/docs/), and others.

The difference between "traditional" Actor systems like Erlang/Akka and "virtual" actor systems like Orleans and NOLA is that the actors are treated as "virtual" entities instead of "physical" ones. Once an actor has been created in NOLA, it exists forever. While an actor may not always actually be "activated" and loaded in memory at any given moment, from an external perspective, NOLA will behave as if it is. This is the "Perpetual existence" concept referred to in the Orleans paper, and what allows virtual actor systems to be "highly available" despite not having supervision trees like Akka and Erlang do.

If an actor is already "activated" on a server in-memory, all requests for that actor will be routed to that server. If the actor is not currently activated on any server, then it will be activated "on-demand" as soon as a message is sent to it. As a user of NOLA, you never have to think about "where" an Actor lives, or how to reach it. NOLA abstracts that all away ("location transparency"). As long as you know the ID of an actor, you can always reach it, and NOLA will ensure it is always reachable.

There is one important difference between NOLA and the original Orleans paper. Under normal circumstances, Orleans guarantees that only one instance of a given actor is activated in the system at any moment in time. However, Orleans is an "HA" system and in the presence of certain failures, user will be able to "observe" multiple instances of an actor in a non-linearizable fashion. Consider a simple actor that when invoked increments an in-memory counter and returns the current count. A completely linearizable system would ensure that a single-threaded external caller invoking the `inc` function in a loop would always yield a result that is equal to the previously observed value + 1, or 0. For example, the following sequence is correct and linearizable:

[0, 1, 2, 3, 4, 5, 0, 1, 2, 0, 1, 0, 1, 2, 3] ("CP", linearizable, what NOLA provides)

Everytime the counter resets to 0, that indicates that the system GC'd the actor and it was re-activated on subsequent invocation or that there was some kind of system failure (server crash, etc) and the actor had to be reactivated on a different server. Regardless, this behavior is consistent and linearizable. The actor's execution behaves as if it was a single-threaded entity despite the fact that it is operating in a dynamic and potentially turbulent distributed environment.

However, the following observed sequence would _not_ be considered linearizable:

[0, 1, 2, 0, 3, 1, 2, 4] ("AP", non-linearizable, what Orleans provides)

This is not linearizable because a failure in the system has "leaked" externally. The caller is able to observe, even if just for a moment, that there are two activated instances of the same actor.

Orleans is a non-linearizable, eventually consistent "AP" system. NOLA, however, is a CP system that guarantees external consistency / linearizability from an external callers perspective. This means that NOLA will become unavailable in some failure scenarios in which Orleans would have continued to function. However, in exchange, NOLA can be used as a primitive for building distributed systems that demand correctness, whereas Orleans cannot. In practice we think this makes NOLA significantly more versatile, and enables a wide variety of use-cases that could not be built using Orleans without leveraging additional external distributed systems as dependencies.

We believe this is the right trade-off because in practice most distributed systems care about correctness. Trying to use a system like Orleans and layer strong consistency on top of it is extremely difficult, complex, and error prone. Instead, we believe that the underlying virtual actor framework should provide linearizability so the application developer can focus their attention elsewhere.

Note that while NOLA is a "CP" system in the [CAP](https://en.wikipedia.org/wiki/CAP_theorem) sense, it is still highly available and fault tolerant. Individual server failures are handled automatically via NOLA's heartbeating and discovery system, and actors that were activated on a server that failed or crashed will be automatically migrated to a new healthy server on subsequent invocation. In addition, the "hard parts" of guaranteeing correctness and linearizability in NOLA are offloaded to [FoundationDB](https://github.com/apple/foundationdb) which is a strongly consistent distributed database that is well known for its [reliability and fault tolerance](https://apple.github.io/foundationdb/fault-tolerance.html).

NOLA diverges from Olreans in one more important way: Every NOLA actor gets its own durable, per-actor transactional KV store. In this sense it resembles [Cloudflare's Durable Objects](https://developers.cloudflare.com/workers/learning/using-durable-objects/) more than Orleans.

NOLA is still in early phases of development; however, the goal is to develop it into a production grade system that could be deployed to datacenter environments and used as a powerful primitive for building distributed applications.

# Features

NOLA is still a work in progress and arguably not yet production ready. Its current capabilities are best understood via the tests in `environment_test.go`. However, NOLA already has a large number of features and functionality implemented:

1. Actors can be written in pure Go (using NOLA as an embedded library) or in any language that can be compiled to WASM and uploaded to NOLA at runtime. A single application using NOLA can run a mixture of Go and WASM actors. The Go actors can even invoke the WASM actors and vice versa without issue.
2. Actors can be instantiated on-demand and "live" forever (or until they're manually removed).
3. Communication with actors happens via RPC.
4. Actor execution is single-threaded and all RPCs/Invocations execute atomically.
5. Actors can spawn new actors and invoke functions on other actors.
6. Every actor comes with its own built-in durable and fully transactional KV storage.
7. The system is externally consistent / linerizable / strongly consistent in the same way that Cloudflare durable objects are. See our [formal model](https://github.com/richardartoul/nola/tree/master/proofs/stateright/activation-cache) for more details.
8. Actors are "cheap". Millions of them can be created, and they can be evicted from memory when they're inactive (not actively receiving RPCs or doing useful work). An inactive actor will be "activated" on-demand as soon as someone issues an RPC for it.
9. By default, an Actor will only ever have a single live activation in the system at any given moment. In effect, every Actor is an HA singleton that NOLA ensures is always available. Inactive actors are automatically GC'd by the system until they become active again.
10. The system self heals by automatically detecting failed servers and removing them from the cluster. Actors on the failed server are automatically reactived on a healthy server on their next invocation/RPC.
11. An intelligent control plane that assigns individual actors to servers based relevant criteria like load, memory usage, and locality of communication. Note that this feature is only partially implemented. Right now, NOLA will simply try and "load balance" the number of actors evenly across all available servers, but it will not take CPU or memory usage into account at all.
12. Orleans-style timers such that activated actors can schedule function invocations to run at sometime in the future or on a regular basis.

# Key Technologies

1. [WASM/WASI](https://webassembly.org/)
2. [wazero](https://wazero.io/)
3. [WAPC protocol](https://wapc.io/)
4. [tinygo](https://tinygo.org/)
5. [FoundationDB](https://www.foundationdb.org/)

# Correctness

## Proof of Linearizability

We don't have a formal proof of NOLA's linearizability, but we do have a [formal model](https://github.com/richardartoul/nola/tree/master/proofs/stateright/activation-cache) implemented in [StateRight](https://github.com/stateright/stateright) that attempts to verify this property ([and even caught a real bug!](https://github.com/richardartoul/nola/issues/23)). Of course there may still be bugs in our implementation that could lead to failures of linearizability.

One thing to keep in mind is that NOLA can only provide linearizability in terms of operations _within_ the system. Currently this is limited to actor invocations (function calls), and the [integrated per-actor KV storage](https://github.com/richardartoul/nola/issues/30). Operations which have external side-effects outside of NOLA are not guaranteed to be linerizable.

# WASM and Library Support

NOLA has two ways in which it can be used:

1. As an embedded library for writing distributed application using Go. In this mode, user's can implement whatever logic they want in their actor and pick and choose which features of NOLA they want to use.
2. As a distributed system / server / cluster that can be used as a dynamic environment in which to run WASM-based actors.

## WASM Support

NOLA leverages [WASM/WASI](https://webassembly.org/) as one of the ways in which virtual actor can be created and executed in a distributed environment. Currently, NOLA uses the excellent [wazero](https://wazero.io/) library for WASM compilation/execution, although additional runtimes may be added in the future. Actors can be written in any language that can be compiled to WASM. Communication with actors happens via RPC, so WASM modules must implement the [WAPC protocol](https://wapc.io/). Implementing this protocol is straightforward, and libraries already exist in a variety of languages. For example, writing a WAPC-compatible actor in Go is just a few lines of code with the [wapc-go](https://github.com/wapc/wapc-go) library:

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

The program above can be compiled with [tinygo](https://tinygo.org/) and [uploaded directly to NOLA's module registry](https://github.com/richardartoul/nola/blob/master/scripts/playground/register_module.sh). Once that is done, an arbitrary number of actors can be instantiated from that module. Note that WASM actors do _not_ need to be "prebaked" into NOLA. They can be "uploaded" and executed at runtime.

### WASM Playground

The NOLA repository contains a simple playground for local experimentation with the WASM functionality. In the root of the directory run:

```bash
make run-server-local-registry
```

to start a local instance of the NOLA server backed by an in-memory registry.

Next, run:

```bash
make run-wasm-playground
```

which will execute `./scripts/playground/basic.sh`. This will register a module in the `playground` namespace, instantiate a single actor named: `test_utils_actor_1`, and then invoke the `inc` function on the actor a few times and print the result.

## Library Support

TODO: This is implemented, but I don't have a concrete code sample published yet. Will publish an example soon.

# Benchmarks

TODO: Update this section with the new benchmarks that include communication with FoundationDB, etc.

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
