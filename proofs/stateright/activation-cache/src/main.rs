use derive_more::Add;
use stackvector::VecLike;
use stateright::actor::*;
use stateright::util::HashableHashMap;
use stateright::{Checker, Expectation, Model};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

// used to construct history
// buy appending the counter value to every time
// a Counter message is ready to be sent out
fn record_out<C, H>(_cfg: &C, history: &H, env: Envelope<&ActorMsg>) -> Option<H>
where
    H: Clone + VecLike<u32>,
{
    match env.msg {
        Counter { counter } => {
            let mut history = history.clone();
            history.push(*counter);
            Some(history)
        }
        _ => None,
    }
}

// check the sequence of counter values
// it should only either increase or reset back to 1
// a valid sequence can look like 1 2 3 1 2 1 1 1
// while 1 2 2 3 3 4 is invalid
fn check_history(history: Vec<u32>) -> bool {
    if history.len() > 0 {
        let mut previous = history[0];
        for i in history {
            if previous > i && i != 1 {
                return false;
            }
            if previous == i && i != 1 {
                return false;
            }
            if previous + 1 != i && i != 1 {
                return false;
            }
            previous = i;
        }
    }

    return true;
}

fn invoke_references(
    env: &mut Env,
    reference: &Reference,
    actor_id: &u32,
    rejections: &mut HashableHashMap<u32, BTreeSet<u32>>,
    vs: &VersionStamp,
) -> Option<ActorMsg> {
    if env.heartbeat_state.heartbeat_result.version_stamp == VersionStamp(0) {
        return Some(InvokeError {
            actor_id: *actor_id,
            server_id: reference.server_id,
            heartbeat_version_stamp: VersionStamp(0),
            version_stamp: *vs,
            msg: "heartbeat version stamp is 0".to_string(),
        });
    }

    // IP re-assigment is out of the scope of this model so this if condition will never check as true
    if env.server_id != reference.server_id {
        return Some(InvokeError {
            actor_id: *actor_id,
            server_id: reference.server_id,
            heartbeat_version_stamp: VersionStamp(0),
            version_stamp: *vs,
            msg: "cannot fulfill for this server".to_string(),
        });
    }

    // reject invocation if either heartbeat stamp is outadated
    // of if the server version changed (which means that the server has missed a heartbeat)
    // and there is a possibility that the actor is now activated on a new server
    if (env.heartbeat_state.heartbeat_result.version_stamp
        + env.heartbeat_state.heartbeat_result.heartbeat_ttl
        < *vs)
        || reference.server_version != env.heartbeat_state.server_version // TRY IT comment out the server version comparison condition to obsereve violations in the model
    {
        // this should be rejected and the activation cache will eventually expire
        // there is no TTL in the model we remove it right away
        env.activation_cache.remove(actor_id);

        env.counter = 0;

        // record this occurance for state discovery
        match rejections.get_mut(&actor_id) {
            Some(server_ids) => {
                server_ids.insert(reference.server_id);
            }
            None => {
                let mut server_ids = BTreeSet::new();
                server_ids.insert(reference.server_id);
                rejections.insert(*actor_id, server_ids);
            }
        }

        return Some(InvokeError {
            actor_id: *actor_id,
            server_id: reference.server_id,
            heartbeat_version_stamp: env.heartbeat_state.heartbeat_result.version_stamp,
            version_stamp: *vs,
            msg: "server heartbeat version stamp < version stamp".to_string(),
        });
    }

    env.counter = env.counter + 1;

    return None;
}

#[derive(Copy, Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct ServerState {
    server_id: u32,
    last_heartbeated_at: Time,
    heartbeat_state: HeartbeatState,
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Copy)]
pub struct HeartbeatState {
    num_activated_actors: u32,
    heartbeat_result: HeartbeatResult,
    server_version: u32,
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Copy)]
pub struct HeartbeatResult {
    version_stamp: VersionStamp,
    heartbeat_ttl: VersionStamp,
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Add, PartialOrd, Copy)]
struct Time(u32);

impl Time {
    fn increment(&mut self) -> () {
        *self = *self + Time(1)
    }
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Add, PartialOrd, Copy)]
struct VersionStamp(u32);

impl VersionStamp {
    fn increment(&mut self) -> () {
        *self = *self + VersionStamp(1)
    }
}

impl ServerState {
    fn new(server_id: u32) -> ServerState {
        ServerState {
            server_id: server_id,
            last_heartbeated_at: Time(0),
            heartbeat_state: HeartbeatState {
                num_activated_actors: 0,
                heartbeat_result: HeartbeatResult {
                    version_stamp: VersionStamp(0),
                    heartbeat_ttl: VersionStamp(5),
                },
                server_version: 0,
            },
        }
    }
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
struct Env {
    activation_cache: HashableHashMap<u32, Reference>,
    server_id: u32,
    heartbeat_state: HeartbeatState,
    counter: u32,
}

impl Env {
    fn new(server_id: &u32) -> Env {
        Env {
            server_id: *server_id,
            activation_cache: HashableHashMap::new(),
            heartbeat_state: HeartbeatState {
                num_activated_actors: 0,
                heartbeat_result: HeartbeatResult {
                    version_stamp: VersionStamp(0),
                    heartbeat_ttl: VersionStamp(5),
                },
                server_version: 0,
            },
            counter: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Copy)]
struct Reference {
    server_id: u32,
    server_version: u32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ActorMsg {
    NewEnvironment {
        server_id: u32,
    },
    StartHeartbeat {
        server_id: u32,
    },
    HeartbeatTransaction {
        server_id: u32,
        is_alive: bool,
    },
    HeartbeatTransactionOK {
        server_id: u32,
        heartbeat_result: HeartbeatResult,
        is_alive: bool,
        version_stamp: VersionStamp,
        last_heartbeated_at: Time,
        server_version: u32,
    },
    GetVersion {
        req_id: u32,
        actor_id: u32,
        env: u32,
    },
    Version {
        req_id: u32,
        version_stamp: VersionStamp,
        actor_id: u32,
        env: u32,
    },
    Invoke {
        actor_id: u32,
        env: u32,
    },
    InvokeError {
        actor_id: u32,
        server_id: u32,
        heartbeat_version_stamp: VersionStamp,
        version_stamp: VersionStamp,
        msg: String,
    },
    EnsureActivation {
        req_id: u32,
        actor_id: u32,
        vs: VersionStamp,
        env: u32,
    },
    ActorReference {
        req_id: u32,
        actor_id: u32,
        reference: Reference,
        vs: VersionStamp,
        env: u32,
    },
    EnsureActivationError {
        actor_id: u32,
        msg: String,
    },
    Counter {
        counter: u32,
    },
}
use ActorMsg::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ActorState {
    EnvironmentState {
        time: Time,
        req_increment: u32,
        newest_req_id: u32,
        envs: HashableHashMap<u32, Env>,

        rejections: HashableHashMap<u32, BTreeSet<u32>>,
        invoked_on_diff_server: HashableHashMap<Vec<u32>, u32>,
        invoked_on_same_server: HashableHashMap<Vec<u32>, u32>,
        two_servers_per_actor: bool,
    },
    HeartbeatRoutineState {},
    FDBState {
        version_stamp: VersionStamp,
        servers: HashableHashMap<u32, ServerState>,
        activations: HashableHashMap<u32, Reference>,
        time: Time,
        newest_req_id: u32,
        activated_more_than_once: bool,
    },
}
use ActorState::*;

#[derive(PartialEq, Clone, Debug, Eq, Hash)]
enum Roles {
    Environment {
        num_servers: u32,
        num_invocations: u32,
    },
    HeartbeatRoutine,
    FDB,
}
use Roles::*;

impl Actor for Roles {
    type Msg = ActorMsg;
    type State = ActorState;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match &self {
            Environment {
                num_servers,
                num_invocations,
            } => {
                for i in 0..*num_servers {
                    o.send(id, NewEnvironment { server_id: i + 1 });
                }

                for _ in 0..*num_invocations {
                    for server_id in 0..*num_servers {
                        o.send(
                            id,
                            Invoke {
                                actor_id: 1,
                                env: server_id + 1,
                            },
                        );
                    }
                }

                EnvironmentState {
                    time: Time(0),
                    req_increment: 0,

                    envs: HashableHashMap::new(),
                    rejections: HashableHashMap::new(),
                    invoked_on_diff_server: HashableHashMap::new(),
                    invoked_on_same_server: HashableHashMap::new(),
                    two_servers_per_actor: false,
                    newest_req_id: 0,
                }
            }
            HeartbeatRoutine => HeartbeatRoutineState {},
            FDB => FDBState {
                servers: HashableHashMap::new(),
                activations: HashableHashMap::new(),
                version_stamp: VersionStamp(0),
                activated_more_than_once: false,
                time: Time(0),
                newest_req_id: 0,
            },
        }
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match &self {
            Environment { .. } => match msg {
                NewEnvironment { server_id } => {
                    // ref: https://github.com/richardartoul/nola/blob/f50a94a7b69b4e64f10bc47905ac0848b953994a/virtual/environment.go#L55
                    if let EnvironmentState { time, envs, .. } = state.to_mut() {
                        time.increment();

                        envs.insert(server_id, Env::new(&server_id));

                        o.send(
                            0.into(),
                            HeartbeatTransaction {
                                server_id,
                                is_alive: true,
                            },
                        );
                    }

                    // start heartbeat routine
                    o.send(1.into(), StartHeartbeat { server_id });
                }
                Invoke { actor_id, env } => {
                    // ref: https://github.com/richardartoul/nola/blob/f50a94a7b69b4e64f10bc47905ac0848b953994a/virtual/environment.go#L130
                    // NOTE: namespace differentiation is ommitted

                    if let EnvironmentState {
                        time,
                        req_increment,
                        ..
                    } = state.to_mut()
                    {
                        time.increment();
                        *req_increment = *req_increment + 1;

                        // get version stamp
                        o.send(
                            0.into(),
                            GetVersion {
                                actor_id,
                                env,
                                req_id: *req_increment,
                            },
                        );
                    }
                }
                Version {
                    version_stamp,
                    actor_id,
                    env,
                    req_id,
                } => {
                    if let EnvironmentState { newest_req_id, .. } = state.as_ref() {
                        if newest_req_id > &req_id {
                            return;
                        }
                    }

                    if let EnvironmentState {
                        time,
                        envs,
                        rejections,
                        newest_req_id,
                        req_increment,
                        ..
                    } = state.to_mut()
                    {
                        *newest_req_id = req_id;
                        *req_increment = *req_increment + 1;
                        time.increment();

                        match envs.get_mut(&env) {
                            Some(env) => {
                                match env.activation_cache.get(&actor_id) {
                                    // if exists in cache
                                    Some(&server_id) => {
                                        // ref: invokeReferences @ TODO
                                        // ref: InvokeLocal @ TODO
                                        match invoke_references(
                                            env,
                                            &server_id,
                                            &actor_id,
                                            rejections,
                                            &version_stamp,
                                        ) {
                                            Some(msg) => {
                                                o.send(src, msg);
                                            }
                                            None => {
                                                o.send(
                                                    src,
                                                    Counter {
                                                        counter: env.counter,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                    // activate if not in cache
                                    None => {
                                        // ensure activation
                                        // ref: EnsureActivation @ https://github.com/richardartoul/nola/blob/f50a94a7b69b4e64f10bc47905ac0848b953994a/virtual/registry/kv_registry.go#L216

                                        // create new activation or use existing if server is alive
                                        o.send(
                                            src,
                                            EnsureActivation {
                                                actor_id,
                                                vs: version_stamp,
                                                req_id: *req_increment,
                                                env: env.server_id,
                                            },
                                        );
                                    }
                                }
                            }
                            None => {}
                        }
                    }
                }
                ActorReference {
                    actor_id,
                    reference,
                    vs,
                    env,
                    req_id,
                } => {
                    if let EnvironmentState { newest_req_id, .. } = state.as_ref() {
                        if newest_req_id > &req_id {
                            return;
                        }
                    }

                    if let EnvironmentState {
                        envs,
                        rejections,
                        invoked_on_diff_server,
                        invoked_on_same_server,
                        two_servers_per_actor,
                        time,
                        newest_req_id,
                        ..
                    } = state.to_mut()
                    {
                        *newest_req_id = req_id;
                        time.increment();

                        match envs.get_mut(&env) {
                            Some(env) => {
                                // save to cache
                                let _ = env.activation_cache.insert(actor_id, reference);

                                match invoke_references(env, &reference, &actor_id, rejections, &vs)
                                {
                                    Some(msg) => {
                                        o.send(src, msg);
                                    }
                                    None => {
                                        o.send(
                                            src,
                                            Counter {
                                                counter: env.counter,
                                            },
                                        );
                                    }
                                }

                                // check if the reference just created somehow exists in cache (this shouldn't)
                                match env.activation_cache.get(&actor_id) {
                                    Some(cached_server) => {
                                        if cached_server.server_id != reference.server_id {
                                            *two_servers_per_actor = true;
                                        }
                                    }
                                    None => {}
                                }
                            }
                            None => {}
                        }

                        // record invocation after rejection
                        match rejections.get(&actor_id) {
                            Some(server_ids) => {
                                if server_ids.contains(&reference.server_id) {
                                    invoked_on_same_server
                                        .entry(vec![actor_id, reference.server_id])
                                        .and_modify(|count| *count += 1)
                                        .or_insert(1);
                                } else {
                                    invoked_on_diff_server
                                        .entry(vec![actor_id, reference.server_id])
                                        .and_modify(|count| *count += 1)
                                        .or_insert(1);
                                }
                            }
                            None => {}
                        }
                    }
                }
                HeartbeatTransactionOK {
                    server_id,
                    server_version,
                    heartbeat_result,
                    ..
                } => {
                    if let EnvironmentState { envs, time, .. } = state.to_mut() {
                        time.increment();

                        match envs.get_mut(&server_id) {
                            Some(env) => {
                                env.heartbeat_state = HeartbeatState {
                                    num_activated_actors: 0,
                                    heartbeat_result,
                                    server_version,
                                };
                            }
                            None => {}
                        }
                    }
                }
                _ => {}
            },
            HeartbeatRoutine { .. } => match msg {
                StartHeartbeat { server_id } => {
                    o.send(
                        0.into(),
                        HeartbeatTransaction {
                            server_id,
                            is_alive: true,
                        },
                    );
                }
                _ => {}
            },
            FDB { .. } => match msg {
                GetVersion {
                    actor_id,
                    req_id,
                    env,
                } => {
                    if let FDBState {
                        version_stamp,
                        time,
                        ..
                    } = state.to_mut()
                    {
                        version_stamp.increment();
                        time.increment();

                        o.send(
                            src,
                            Version {
                                version_stamp: *version_stamp,
                                actor_id,
                                env,
                                req_id,
                            },
                        );
                    }
                }
                HeartbeatTransaction {
                    server_id,
                    is_alive,
                } => {
                    if let FDBState {
                        servers,
                        version_stamp,
                        time,
                        ..
                    } = state.to_mut()
                    {
                        version_stamp.increment();
                        time.increment();

                        let mut server_version = 0;

                        if is_alive {
                            match servers.get_mut(&server_id) {
                                Some(server_state) => {
                                    if server_state.last_heartbeated_at + Time(5) <= *time {
                                        server_version =
                                            server_state.heartbeat_state.server_version + 1
                                    } else {
                                        server_version = server_state.heartbeat_state.server_version
                                    };

                                    server_state.last_heartbeated_at = *time;
                                    server_state.heartbeat_state.heartbeat_result.version_stamp =
                                        *version_stamp;
                                    server_state.heartbeat_state.server_version = server_version;
                                }
                                None => {
                                    let mut server_state = ServerState::new(server_id);
                                    server_state.last_heartbeated_at = *time;
                                    server_state.heartbeat_state.heartbeat_result.version_stamp =
                                        *version_stamp;
                                    servers.insert(server_id, server_state);
                                }
                            }
                        }

                        o.send(
                            2.into(),
                            HeartbeatTransactionOK {
                                server_id,
                                heartbeat_result: HeartbeatResult {
                                    version_stamp: *version_stamp,
                                    heartbeat_ttl: VersionStamp(5),
                                },
                                is_alive,
                                version_stamp: *version_stamp,
                                server_version,
                                last_heartbeated_at: *time,
                            },
                        )
                    }
                }
                EnsureActivation {
                    actor_id,
                    vs,
                    req_id,
                    env,
                } => {
                    if let FDBState {
                        time,
                        version_stamp,
                        ..
                    } = state.to_mut()
                    {
                        time.increment();
                        version_stamp.increment();
                    }

                    // ref: EnsureActivated @ https://github.com/richardartoul/nola/blob/b1c5dff0f71a46c903ab7c526b1672c738089344/virtual/registry/kv_registry.go#L262

                    // check if activation exists and server is alive
                    let mut activation_exists = false;
                    let mut server_alive = false;
                    let mut server_id = 0;
                    let mut activated_reference = Reference {
                        server_id: 0,
                        server_version: 0,
                    };

                    let mut live_server_id = 0;
                    let mut live_server_ids: BTreeSet<u32> = BTreeSet::new();
                    let mut activated_count: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();

                    if let FDBState {
                        activations,
                        servers,
                        time,
                        ..
                    } = state.as_ref()
                    {
                        // find if activation exists for actor
                        for (activated_actor_id, activated_server) in activations {
                            if actor_id == *activated_actor_id {
                                activation_exists = true;
                                server_id = activated_server.server_id;
                                activated_reference = *activated_server;
                            }
                        }

                        // find out if the server is alive
                        for (existing_server_id, server_state) in servers {
                            if server_id == *existing_server_id
                                && server_state.last_heartbeated_at + Time(5) > *time
                            {
                                server_alive = true;
                            }

                            // find all live servers and sort them by number of actors on them
                            if server_state.last_heartbeated_at + Time(5) > *time {
                                live_server_ids.insert(*existing_server_id);

                                match activated_count
                                    .get_mut(&server_state.heartbeat_state.num_activated_actors)
                                {
                                    Some(server_ids) => {
                                        server_ids.insert(*existing_server_id);
                                    }
                                    None => {
                                        let mut s = BTreeSet::new();
                                        s.insert(*existing_server_id);
                                        activated_count.insert(
                                            server_state.heartbeat_state.num_activated_actors,
                                            s,
                                        );
                                    }
                                }
                            }
                        }

                        // get a live server with least amount of actors running
                        // to use for new activation if the server is dead
                        if activated_count.len() > 0 {
                            let least_activated_actors = activated_count.iter().next().unwrap();
                            live_server_id = *least_activated_actors.1.iter().next().unwrap();
                        }
                    }

                    if activation_exists && server_alive {
                        o.send(
                            src,
                            ActorReference {
                                actor_id,
                                reference: activated_reference,
                                vs,
                                req_id,
                                env,
                            },
                        )
                    } else {
                        // create a new activation
                        if live_server_id > 0 {
                            if let FDBState {
                                activations,
                                servers,
                                ..
                            } = state.to_mut()
                            {
                                match servers.get_mut(&live_server_id) {
                                    Some(server) => {
                                        server.heartbeat_state.num_activated_actors =
                                            server.heartbeat_state.num_activated_actors + 1;

                                        let new_reference = Reference {
                                            server_id: server.server_id,
                                            server_version: server.heartbeat_state.server_version,
                                        };

                                        activations
                                            .entry(actor_id)
                                            .and_modify(|v| *v = new_reference)
                                            .or_insert(new_reference);

                                        o.send(
                                            src,
                                            ActorReference {
                                                actor_id,
                                                reference: new_reference,
                                                vs,
                                                req_id,
                                                env,
                                            },
                                        )
                                    }
                                    None => {}
                                }
                            }
                        } else {
                            o.send(
                                src,
                                EnsureActivationError {
                                    actor_id,
                                    msg: "No live servers".to_string(),
                                },
                            )
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info")); // `RUST_LOG=${LEVEL}` env variable to override

    let mut args = pico_args::Arguments::from_env();

    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let thread_count = args.opt_free_from_str()?.unwrap_or(1);
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!(
                "Exploring the state space with {} threads on {}.",
                thread_count, address
            );

            let checker = ActorModel::new((), Vec::new())
                .actor(FDB)
                .actor(HeartbeatRoutine)
                .actor(Environment {
                    num_servers: 2,
                    num_invocations: 4,
                })
                .init_network(Network::new_ordered([]))
                .property(Expectation::Always, "increment counter history check passes", |_, state| {
                    check_history(state.history.clone())
                })
                .property(
                    Expectation::Sometimes,
                    "invocation is rejected",
                    |_, state| {
                        state.actor_states.iter().any(|s| {
                            if let EnvironmentState { rejections, .. } = s.as_ref() {
                                for (_, server_ids) in rejections {
                                    if server_ids.len() > 0 {
                                        return true;
                                    }
                                }
                                false
                            } else {
                                false
                            }
                        })
                    },
                )
                .property(
                    Expectation::Sometimes,
                    "invoked on a diff server after rejection",
                    |_, state| {
                        state.actor_states.iter().any(|s| {
                            if let EnvironmentState {
                                invoked_on_diff_server,
                                ..
                            } = s.as_ref()
                            {
                                for (_, count) in invoked_on_diff_server {
                                    if *count > 0 {
                                        return true;
                                    }
                                }
                                false
                            } else {
                                false
                            }
                        })
                    },
                )
                .property(
                    Expectation::Sometimes,
                    "invoked on the same server after rejection",
                    |_, state| {
                        state.actor_states.iter().any(|s| {
                            if let EnvironmentState {
                                invoked_on_same_server,
                                ..
                            } = s.as_ref()
                            {
                                for (_, count) in invoked_on_same_server {
                                    if *count > 0 {
                                        return true;
                                    }
                                }
                                false
                            } else {
                                false
                            }
                        })
                    },
                )
                .record_msg_out(record_out)
                .checker()
                .threads(num_cpus::get())
                .serve(address);

            checker.assert_properties();
        }
        _ => {
            println!("USAGE:");
            println!("  ./increment check [THREAD_COUNT]");
            println!("  ./increment check-sym [THREAD_COUNT] [full|sorted]");
            println!("  ./increment explore [THREAD_COUNT] [ADDRESS]");
        }
    }

    Ok(())
}
