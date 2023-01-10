# Overview

The semaphore example demonstrates how to create a distributed semaphore with acquire/release semantics and a built-in TTL for the acquire. This could be used for ratelimiting in distributed environments. For example, if you wanted to throttle the maximum number of allowed concurrent requests in a multi-tenant environment based on the caller's user ID and the request type. This can useful in scenarios where you care more maximum *concurrent* resource utilization than you do about the *rate* of requests.

## Scaling

The sample actor is single-threaded. However, scaling this application is trivial: create a new actor for every tenant. NOLA will automatically GC actors for inactive tenants, and NOLA supports having millions of actors actively loaded into memory if needed.

## Housecleaning

In some cases, caller's will crash or fail before they're able to call release() after their acquire(). This example leverages NOLA's built-in timer functionality to run a housecleaning routine once every 10s. The housecleaning routine scans all in-memory requests that have acquired but not released yet and checks if they're expired. If they are expired, it releases their resources. Housecleaning scales just like the actor itself does. Each actor will get their own housecleaning routine so it parallelizes trivially with the number of tenants. In addition, NOLA timer's only run while the actor is active and loaded in memory, so once a tenant stops using the system and their actor is GC'd from memory, their housecleaningr routine will be cleaned up as well.