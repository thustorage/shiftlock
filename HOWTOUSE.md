# Description of this repository

This file describes the structure of this repository and the functionalities of the individual files in it.

If you want to reuse ShiftLock for your own purposes, then you may want to read this file.

## Directory structure

Only 3 subdirectories in this repository are important, and their structures are as follows:

```
.
├── src
│   ├── app
│   ├── baselines
│   ├── bin
│   ├── shiftlock
│   ├── lib.rs
│   ├── macros.rs
│   ├── main.rs
│   └── utils
├── scripts
│   ├── ae
│   ├── distribute-traces.sh
│   ├── run-XXX.sh
│   ├── trace_analyze.py
│   └── utils
└── traces
```

- **src** accommodates the source code of ShiftLock and baselines.
- **script** accommodates experiment script files.
- **traces** accommodates traces used in evaluation, but be careful: files in it are NOT used directly!

### Source code

`src` is, undoubtfully, the most important part of this repository.
Files in it are arranged in different Rust modules and has different functionalities.

#### The "hello-world" example

For newcomers, we recommend that you look at `src/main.rs` first.
In this file we have a **minimum implementation of a test program of ShiftLock**: it starts a lock server and some clients (default to 8) locally, runs ShiftLock for some time (default to 10s), and outputs the goodput.
It also serves as a hello-world example for AE reviewers.
If you want to reuse ShiftLock's code, the first thing you need to do is to run this file:

```shell
cargo run --bin shiftlock
```

After you successfully run `main.rs`, we recommend that you take a quick look of the code in it, especially about how we accept cmdline arguments with `clap` and how we implement `worker`.
Other exectuables use almost the same techniques.

#### Implementation

You can find ShiftLock's implementation in the `src/shiftlock` directory.
Most implementation details lies in `src/shiftlock/mod.rs`, while some minor functionalities are implemented in `src/shiftlock/ahcache.rs` and `src/shiftlock/consrec.rs`.
There are detailed comments describing what each module/struct/function will do (because Rust encourages us to do so), and we expect that you can grasp the essentials of the code quickly.

We have prepared many baseline systems to compare them against ShiftLock.
You may find them in the `src/baselines` directory.
The files are named after the baselines:

- `cas.rs`: CAS lock, with backoff strategy taken from [SMART (ASPLOS'24)](https://dl.acm.org/doi/10.1145/3617232.3624857).
- `drtm.rs`: [DrTM (SOSP'15)](https://dl.acm.org/doi/10.1145/2815400.2815419).
- `dslr.rs`: [DSLR (SIGMOD'18)](https://dl.acm.org/doi/10.1145/3183713.3196890). _We greatly simplified its implementation because we have extended atomics, which do not carry across field boundaries._
- `mcs.rs`: MCS lock. [Here](https://dl.acm.org/doi/10.1145/103727.103729) is the paper authored by the namers of the lock.
- `rma_rw.rs`: [RMA-RW (HPDC'16)](https://dl.acm.org/doi/10.1145/2907294.2907323). We translated the original C code into Rust.
- `rpc.rs`: RPC lock.

#### Testbed infrastructure

To evaluate the systems above, some testbed infrastructure is necessary.
We place those code in `src/bin`.
There are 5 files in this directory.
`server.rs` is obviously the lock server; the other four are clients.

The lock server does nothing special.
It allocates a large piece of memory to accommodate lock entries, and start listening on incoming TCP connections from clients.
It also runs several threads to server RPCs from clients.
The cmdline arguments configure the lock count, the lock size, the TCP address to listen on, etc.
Server must be run before clients and never stops after launched, but you can just press Ctrl+C to terminate it whenever you want.

The four clients each has its own purpose.

- `client.rs`: Implements client functions to run all locks. Yes, each lock has its own function.
  You might think: "_why is there so much repetition!?_", but this is our last resort after surrendering in a big battle with Rust's HRTBs.
  Anyway, these functions are quite similar, so you should understand all of them after reading one of them, say `run_shiftlock`.
  Please also pay attention to `parse_workload`: it defines how the client parses a string into a workload specification.
- `client_fallible.rs`: Implements clients for ShiftLock and DSLR. However, to simulate failures, there is a chance that a client simply drops a lock without releasing it.
- `client_redis.rs`: Implements SmallBank by communicating with Redis.
- `zero.rs`: Zeroes the memory provided by the lock server. Useful when you want to run multiple clients sequentially but do not want to restart the server.

It is worth noting that in `client.rs`, to simulate distributed transactions, we invoke some "applications" like:

```rust
// Application.
{
    let app_local = mem.slice(txn.locks().len() * LOCK_RESERVE, 64).unwrap();
    for lock in txn.locks() {
        let app_remote = remote
            .slice(app_offset + lock.id as usize * 64, 64)
            .unwrap();
        MicroRo::new(app_local, app_remote).run(&qp);  // <--
    }
    if !think_time.is_zero() {
        async_busy_wait_dur(think_time).await;
    }
    for lock in txn.locks() {
        let app_remote = remote
            .slice(app_offset + lock.id as usize * 64, 64)
            .unwrap();
        if !lock.shared {
            MicroWo::new(app_local, app_remote).run(&qp);  // <--
        }
    }
}
```

The "applications" simply reads/writes remote memory.
Their implementations can be found in `src/app`.

### Scripts

To reproduce the results in our paper, you can just run the AE scripts in `scripts/ae`.
The `scripts/ae/expX.sh` scripts includes comments that tells you what each script does.

If you happen to read these scripts, you will notice that they actually invokes the scripts located at `scripts`, namely the `run-XXX.sh`.
These scripts are client runners; in other words, you must be running a lock server in another terminal window:

```shell
cargo run --bin server --release
```

They have the following functionalities:

- `run-basic.sh`: Run the executable of `src/bin/client.rs`, using a microbenchmark or a trace file to evaluate a lock.
- `run-counters.sh`: Run the executable of `src/bin/client.rs`, obtaining RNIC hardware counters before and after the run to count how many atomics/reads are there.
- `run-fallible.sh`: Run the executable of `src/bin/client_fallible.rs`, using a microbenchmark to evaluate a fallible lock.
- `run-nlocks.sh`: Run the executable of `src/bin/client.rs` with a configurable number of locks.
- `run-nthreads.sh`: Run the executable of `src/bin/client.rs` with a configurable number of client threads. This and the previous script together serve to evaluate the scalability of locks.
- `run-redis.sh`: Run the executable of `src/bin/client_redis.rs`. **This script also only runs clients; you must launch a Redis instance simultaneously!**

The `scripts/utils/run_once.sh` provides a bash function that reduces some pains by automatically running and stopping a server with tmux.
You may refer to the AE scripts for how to using it.

### Traces

The traces in `traces` does not belong to us -- they are there just for convenience.
As described in the paper, these traces are generated from the tools in [FissLock](https://www.usenix.org/conference/osdi24/presentation/zhang-hanze).
These traces are very large, and you can just generate them by yourself instead of downloading them.

Thank you very much, Mr. Hanze Zhang (the first author of FissLock)!
