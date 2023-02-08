## Purpose of the model

NOLA is a distributed virtual actor system being developed with the goal of becoming a production-grade system for building distributed applications. It is heavily inspired by [Orleans](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf) and [Cloudflare Durable Objects](https://developers.cloudflare.com/workers/learning/using-durable-objects/). It uses WASM/WASI and currently leverages the wazero library for WASM compilation and execution. Actors can be written in any language that can be compiled to WASM and communication with actors is done through RPC using the WAPC protocol.

In contrast, [Orleans](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Orleans-MSR-TR-2014-41.pdf) only guarantees single activation in failure-free scenarios.
This difference can be demonstrated through a simple actor that increments a counter. In a linearizable system, like NOLA, a single-threaded external caller invoking the inc function in a loop would always yield a result that is equal to the previously observed value + 1, or 0. This means the sequence would be consistent and linear, for example [0, 1, 2, 3, 4, 5, 0, 1, 2, 0, 1, 0, 1, 2, 3]. On the other hand, Orleans, as a non-linearizable AP system, can result in a non-linear sequence like [0, 1, 2, 0, 3, 1, 2, 4] in the presence of failures, where a failure in the system has "leaked" externally, allowing the caller to observe two activated instances of the same actor. 

This also means that NOLA trades Orleans-like availabilty for consistency. We believe that this trade-off is the right choice because most distributed systems prioritize correctness and it is difficult, complex, and error prone to add strong consistency to a non-linearizable system like Orleans.

This model was created to verify NOLA's single activation guarantee after the introduction of an activation cache to avoid a database transaction for every actor invocation. The single activation guarantee is a core aspect of NOLA's design, as it is intended for building distributed applications that reqiure high degree of correctness. We use the increment counter actor example to verify correctness.

The activation cache has been intoduced in this [PR](https://github.com/richardartoul/nola/pull/3), and implemented the following logic:

Client:
```
readVersion = fdb.getReadVersion() <-- batch/cache these
server = cache.getActivation()
routeRPC(server, readVersion)
```

Server:
```
backgroundLoop:
    latestHeartbeatVersionStamp, heartbeatTTL = fdb.heartbeat()
server RPC handler:
    if req.versionStamp > lastHeartbeatVersionStamp + heartbeatTLL:
        reject()
```

                                 ┌──────────────┐              ┌─────────┐         ┌─────────┐
                                 │  Environment │              │  Cache  │         │ Storage │
                                 └──────┬───────┘              └─────┬───┘         └────┬────┘
                                        │                            │                  │
                                        │ heartbeat                  │                  │
                                        ├────────────────────────────┼─────────────────►│
                                        │ { versionStamp }           │                  │
                                        │◄───────────────────────────┼──────────────────┤
                                        │                            │                  │
                                        │                            │                  │
               ┌───────────────────┐    │ getReadVersion             │                  │
               │ invocationRequest ├────►────────────────────────────┼─────────────────►│
               └───────────────────┘    │ { versionStamp }           │                  │
                                        │◄───────────────────────────┼──────────────────┤
                                        │ getActorReference          │                  │
                                        ├───────────────────────────►│                  │
                                        │ { references }             │                  │
                                        │◄───────────────────────────┤                  │
                                        │                            │                  │
                                        │                            │                  │
                                  ┌─────┴────┐                       │                  │
                                  │          │ ensureActivation      │                  │
                                  │  Exists  N───────────────────────┼─────────────────►│
                                  │    in    │ { reference }         │                  │
                                  │  cache?  │◄──────────────────────┼──────────────────┤
                                  │          │ activationCache.Set   │                  │
                                  │          ├──────────────────────►│                  │
                                  └─────┬────┘                       │                  │
                                        │                            │                  │
                                        Y                            │                  │
                                        │                            │                  │
                            ┌───────────┴───────────┐                │                  │
                            │      versionStamp     │                │                  │
                            │           <           │                │                  │
                            │  lastHeartbeat + TTL  │                │                  │
                            └─────┬───────────┬─────┘                │                  │
                                  │           │
                                  Y           N
                                  │           │
                            ┌─────┴───┐   ┌───┴─────┐
                            │ invoke  │   │ reject  │
                            └─────────┘   └─────────┘


The heartbeat version stamp check should ensure that an invocation on a server that missed a heartbeat is rejected, which should in turn ensure that only one instance of an actor can run at a time.

Previously, every invocation of an actor involved a FoundationDB transaction to get the actor refence. The FDB-based implementation would not scale beyond ~ 4000 function calls/s globally. The new implementation trivially scales to 280,000 RPC/s locally:

```
Inputs
    invokeEvery 500ns
    numActors 10
Results
    numInvokes 4246528
    invoke/s 283101.86666666664
    median latency 0
    p95 latency 2.9742334234767007 ms
    p99 latency 13.874292528934026 ms
    p99.9 latency 29.080339799119038 ms
    max latency 64.72121241695221 ms
```

## Findings

During formal verification the following state path was discovered that demonstrated violation of the primary correctness property:

![State path leading to violation](./img/model_result_01.png)

- env1 failed to heartbeat the registry and loses ownership of the actor
- actor is activated on env2
- env1 succeeds to heartbeat after a delay
- env1 receives a request to invoke the actor and proceeds with invocation

This caused two instances of one actor to be activated in two different environments.

This anomaly was addressed by adding the following logic:

- during a heartbeat transaction, `if last_heartbeated_at + ttl < now`, the server version is increased (this signifies that a heartbeat came in after the previous on has expired, which means there was a time where the server was considered dead)
- during the invocation request, the invocation if rejected `if env.server_version != reference.server_version` 

After the server version check was introduced, the correctness property of the model was satisfied.

![Model results](./img/model_result_02.png)

## Running the model

Run:

```bash
cargo run --release explore
```

To explore the state space, go to [localhost:3000](http://localhost:3000/).

To observe the behavior of the model in the presence of violations before the server version comparison was introduced, comment out the server version check during actor invocation. (Search for `TRY IT` in the model code)

More information on Stateright can be found [here](https://github.com/stateright/stateright).