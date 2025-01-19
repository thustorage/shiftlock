use std::{
    array, mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Context as _;
use clap::Parser;
use handlock::{
    utils::{self, Timer, TimerOp},
    Handlock, HandlockAcquirePolicy, HandlockEntry,
};
use quanta::Instant;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;
use redis::{Commands, RedisResult};
use rrddmma::{prelude::*, wrap::RegisteredMem};

/// Lock type.
#[derive(strum::EnumString, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Handlock,
    RedLock,
}

/// Execute Smallbank.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Number of threads.
    #[clap(short, long, default_value = "127.0.0.1:6379")]
    pub redis: SocketAddr,

    /// Server address.
    #[clap(short, long, default_value = "127.0.0.1:31850")]
    pub server: SocketAddr,

    /// Prepare?
    #[clap(long, default_value = "false")]
    pub prepare: bool,

    /// Number of threads.
    #[clap(long, default_value = "48")]
    pub nthreads: usize,

    /// Duration in seconds.
    #[clap(short, long, default_value = "5")]
    pub duration: u64,

    /// Lock type.
    #[clap(short, long, default_value = "Handlock")]
    pub lock: LockType,

    /// Backoff nanoseconds. 0 indicates busy retry.
    #[clap(short, long, default_value = "1862")]
    pub backoff: u64,

    /// RDMA device to use.
    #[clap(long, default_value = "mlx5_0")]
    pub dev: String,

    /// Result output file.
    #[clap(short, long, default_value = None)]
    pub output: Option<String>,
}

const TBL_ACCOUNTS: &str = "accounts";
const TBL_SAVINGS: &str = "savings";
const TBL_CHECKING: &str = "checking";

/// RAII wrapper for RedLock.
struct RedLock<'a> {
    rl: &'a redlock::RedLock,
    lock: redlock::Lock<'a>,
}

impl<'a> RedLock<'a> {
    fn new(rl: &'a redlock::RedLock, name: &str) -> Self {
        let lock = loop {
            let lock = rl.lock(name.as_bytes(), 10);
            if let Ok(Some(lock)) = lock {
                break lock;
            }
        };
        Self { rl, lock }
    }
}

impl Drop for RedLock<'_> {
    fn drop(&mut self) {
        self.rl.unlock(&self.lock);
    }
}

/// Amalgamate.
fn rl_amalgamate(
    con: &mut redis::Connection,
    rl: &redlock::RedLock,
    name0: &str,
    name1: &str,
) -> RedisResult<()> {
    let (uid0, uid1) = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id0: usize = con.zrank(TBL_ACCOUNTS, name0)?;
        let id1: usize = con.zrank(TBL_ACCOUNTS, name1)?;
        Ok(Some((id0, id1)))
    })?;

    let s0 = format!("{}:{}", TBL_CHECKING, uid0);
    let _g0 = RedLock::new(rl, &format!("lock_{}", s0));

    let c1 = format!("{}:{}", TBL_SAVINGS, uid1);
    let _g1 = RedLock::new(rl, &format!("lock_{}", c1));

    let sb0: i64 = con.get(&s0)?;
    let cb1: i64 = con.get(&c1)?;

    con.set::<_, _, ()>(s0, 0)?;
    con.set::<_, _, ()>(c1, sb0 + cb1)?;

    Ok(())
}

/// DepositChecking.
fn rl_deposit_checking(
    con: &mut redis::Connection,
    rl: &redlock::RedLock,
    name: &str,
) -> RedisResult<()> {
    let amount = 2;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let c = format!("{}:{}", TBL_CHECKING, uid);
    let _g = RedLock::new(rl, &format!("lock_{}", c));

    let checking: i64 = con.get(&c)?;
    con.set::<_, _, ()>(c, checking + amount)?;

    Ok(())
}

/// SendPayment.
fn rl_send_payment(
    con: &mut redis::Connection,
    rl: &redlock::RedLock,
    name0: &str,
    name1: &str,
) -> RedisResult<()> {
    let amount = 5;
    let (uid0, uid1) = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id0: usize = con.zrank(TBL_ACCOUNTS, name0)?;
        let id1: usize = con.zrank(TBL_ACCOUNTS, name1)?;
        Ok(Some((id0, id1)))
    })?;

    let c0 = format!("{}:{}", TBL_CHECKING, uid0);
    let c1 = format!("{}:{}", TBL_CHECKING, uid1);

    let (_g0, _g1) = if c0 < c1 {
        (
            RedLock::new(rl, &format!("lock_{}", c0)),
            RedLock::new(rl, &format!("lock_{}", c1)),
        )
    } else {
        (
            RedLock::new(rl, &format!("lock_{}", c1)),
            RedLock::new(rl, &format!("lock_{}", c0)),
        )
    };

    let cb0: i64 = con.get(&c0)?;

    if cb0 < amount {
        return Ok(());
    }

    let cb1: i64 = con.get(&c1)?;

    con.set::<_, _, ()>(c0, cb0 - amount)?;
    con.set::<_, _, ()>(c1, cb1 + amount)?;

    Ok(())
}

/// Balance.
fn rhl_balance(con: &mut redis::Connection, name: &str) -> RedisResult<i64> {
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let savings = format!("{}:{}", TBL_SAVINGS, uid);
    let checking = format!("{}:{}", TBL_CHECKING, uid);
    let balance = redis::transaction(con, &[&savings, &checking], |con, _| {
        let savings: i64 = con.get(&savings)?;
        let checking: i64 = con.get(&checking)?;
        Ok(Some(savings + checking))
    })?;
    Ok(balance)
}

/// TransactSavings.
fn rl_transact_savings(
    con: &mut redis::Connection,
    rl: &redlock::RedLock,
    name: &str,
) -> RedisResult<()> {
    let amount = 20;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let s = format!("{}:{}", TBL_SAVINGS, uid);
    let _g = RedLock::new(rl, &format!("lock_{}", s));

    let savings: i64 = con.get(&s)?;
    con.set::<_, _, ()>(s, savings + amount)?;

    Ok(())
}

/// WriteCheck.
fn rl_write_check(
    con: &mut redis::Connection,
    rl: &redlock::RedLock,
    name: &str,
) -> RedisResult<()> {
    let amount = 5;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let c = format!("{}:{}", TBL_CHECKING, uid);
    let _g = RedLock::new(rl, &format!("lock_{}", c));

    let checking: i64 = con.get(&c)?;
    if checking < amount {
        return Ok(());
    }

    con.set::<_, _, ()>(c, checking - amount)?;

    Ok(())
}

#[derive(Debug)]
struct HandlockMeta<'a> {
    id: u16,
    policy: HandlockAcquirePolicy,

    qp: &'a Qp,
    dci: &'a mut Qp,
    dct0: &'a Dct,
    dct1: &'a Dct,

    local: MrSlice<'a>,
    remote: MrRemote,
}

const LOCK_RESERVE: usize = 256;

/// Amalgamate.
async fn hl_amalgamate(
    con: &mut redis::Connection,
    meta: HandlockMeta<'_>,
    name0: &str,
    name1: &str,
) -> RedisResult<()> {
    let (uid0, uid1) = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id0: usize = con.zrank(TBL_ACCOUNTS, name0)?;
        let id1: usize = con.zrank(TBL_ACCOUNTS, name1)?;
        Ok(Some((id0, id1)))
    })?;

    let local0 = meta.local.slice(0, LOCK_RESERVE).unwrap();
    let remote0 = meta
        .remote
        .slice(
            (uid0 + NUM_ACCOUNTS) * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l0 = Handlock::new(meta.id, local0, remote0);
    let g0 = l0
        .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
        .await
        .unwrap();

    let local1 = meta.local.slice(LOCK_RESERVE, LOCK_RESERVE).unwrap();
    let remote1 = meta
        .remote
        .slice(
            uid1 * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l1 = Handlock::new(meta.id, local1, remote1);
    let g1 = l1
        .acquire_ex(meta.qp, meta.dci, meta.dct1, meta.policy)
        .await
        .unwrap();

    let s0 = format!("{}:{}", TBL_CHECKING, uid0);
    let c1 = format!("{}:{}", TBL_SAVINGS, uid1);
    let sb0: i64 = con.get(&s0)?;
    let cb1: i64 = con.get(&c1)?;

    con.set::<_, _, ()>(s0, 0)?;
    con.set::<_, _, ()>(c1, sb0 + cb1)?;

    g0.release(meta.qp, meta.dci, meta.dct0);
    g1.release(meta.qp, meta.dci, meta.dct1);

    Ok(())
}

/// DepositChecking.
async fn hl_deposit_checking(
    con: &mut redis::Connection,
    meta: HandlockMeta<'_>,
    name: &str,
) -> RedisResult<()> {
    let amount = 2;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let local0 = meta.local.slice(0, LOCK_RESERVE).unwrap();
    let remote0 = meta
        .remote
        .slice(
            (uid + NUM_ACCOUNTS) * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l0 = Handlock::new(meta.id, local0, remote0);
    let g0 = l0
        .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
        .await
        .unwrap();

    let c = format!("{}:{}", TBL_CHECKING, uid);
    let checking: i64 = con.get(&c)?;
    con.set::<_, _, ()>(c, checking + amount)?;

    g0.release(meta.qp, meta.dci, meta.dct0);

    Ok(())
}

/// SendPayment.
async fn hl_send_payment(
    con: &mut redis::Connection,
    meta: HandlockMeta<'_>,
    name0: &str,
    name1: &str,
) -> RedisResult<()> {
    let amount = 5;
    let (uid0, uid1) = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id0: usize = con.zrank(TBL_ACCOUNTS, name0)?;
        let id1: usize = con.zrank(TBL_ACCOUNTS, name1)?;
        Ok(Some((id0, id1)))
    })?;

    let c0 = format!("{}:{}", TBL_CHECKING, uid0);
    let c1 = format!("{}:{}", TBL_CHECKING, uid1);

    let local0 = meta.local.slice(0, LOCK_RESERVE).unwrap();
    let remote0 = meta
        .remote
        .slice(
            (uid0 + NUM_ACCOUNTS) * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l0 = Handlock::new(meta.id, local0, remote0);

    let local1: MrSlice = meta.local.slice(LOCK_RESERVE, LOCK_RESERVE).unwrap();
    let remote1 = meta
        .remote
        .slice(
            (uid1 + NUM_ACCOUNTS) * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l1 = Handlock::new(meta.id, local1, remote1);

    let (g0, g1) = if uid0 < uid1 {
        let g0 = l0
            .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
            .await
            .unwrap();
        let g1 = l1
            .acquire_ex(meta.qp, meta.dci, meta.dct1, meta.policy)
            .await
            .unwrap();
        (g0, g1)
    } else {
        let g1 = l1
            .acquire_ex(meta.qp, meta.dci, meta.dct1, meta.policy)
            .await
            .unwrap();
        let g0 = l0
            .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
            .await
            .unwrap();
        (g0, g1)
    };

    let cb0: i64 = con.get(&c0)?;
    if cb0 >= amount {
        let cb1: i64 = con.get(&c1)?;
        con.set::<_, _, ()>(c0, cb0 - amount)?;
        con.set::<_, _, ()>(c1, cb1 + amount)?;
    }

    g0.release(meta.qp, meta.dci, meta.dct0);
    g1.release(meta.qp, meta.dci, meta.dct1);
    Ok(())
}

/// TransactSavings.
async fn hl_transact_savings(
    con: &mut redis::Connection,
    meta: HandlockMeta<'_>,
    name: &str,
) -> RedisResult<()> {
    let amount = 20;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let local0 = meta.local.slice(0, LOCK_RESERVE).unwrap();
    let remote0 = meta
        .remote
        .slice(
            uid * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l0 = Handlock::new(meta.id, local0, remote0);
    let g0 = l0
        .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
        .await
        .unwrap();

    let s = format!("{}:{}", TBL_SAVINGS, uid);
    let savings: i64 = con.get(&s)?;
    con.set::<_, _, ()>(s, savings + amount)?;

    g0.release(meta.qp, meta.dci, meta.dct0);

    Ok(())
}

/// WriteCheck.
async fn hl_write_check(
    con: &mut redis::Connection,
    meta: HandlockMeta<'_>,
    name: &str,
) -> RedisResult<()> {
    let amount = 5;
    let uid = redis::transaction(con, &[TBL_ACCOUNTS], |con, _| {
        let id: usize = con.zrank(TBL_ACCOUNTS, name)?;
        Ok(Some(id))
    })?;

    let local0 = meta.local.slice(0, LOCK_RESERVE).unwrap();
    let remote0 = meta
        .remote
        .slice(
            (uid + NUM_ACCOUNTS) * mem::size_of::<HandlockEntry>(),
            mem::size_of::<HandlockEntry>(),
        )
        .unwrap();
    let l0 = Handlock::new(meta.id, local0, remote0);
    let g0 = l0
        .acquire_ex(meta.qp, meta.dci, meta.dct0, meta.policy)
        .await
        .unwrap();

    let c = format!("{}:{}", TBL_CHECKING, uid);
    let checking: i64 = con.get(&c)?;
    if checking >= amount {
        con.set::<_, _, ()>(c, checking - amount)?;
    }

    g0.release(meta.qp, meta.dci, meta.dct0);
    Ok(())
}

const NUM_ACCOUNTS: usize = 1_000_000;
const MIN_BALANCE: f64 = 1e4;
const MAX_BALANCE: f64 = 5e4;

/// Prepare data.
fn prepare(con: &mut redis::Connection, perform: bool) -> anyhow::Result<Vec<String>> {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let seed = array::from_fn(|i| (i * 13 % 256) as u8);
    let mut rng = rand_chacha::ChaCha20Rng::from_seed(seed);

    let mut names = std::collections::HashSet::new();
    while names.len() < NUM_ACCOUNTS {
        let name: String = (0..8)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();
        names.insert(name);
    }

    let mut accounts = vec![];
    let gen = rand_distr::Normal::new((MIN_BALANCE + MAX_BALANCE) / 2.0, 1e4).unwrap();
    for (i, name) in names.into_iter().enumerate() {
        if perform {
            let saving_balance = loop {
                let balance = gen.sample(&mut rand::thread_rng()).round();
                if MIN_BALANCE <= balance && balance <= MAX_BALANCE {
                    break balance;
                }
            };
            let check_balance = loop {
                let balance = gen.sample(&mut rand::thread_rng()).round();
                if MIN_BALANCE <= balance && balance <= MAX_BALANCE {
                    break balance;
                }
            };

            con.zadd::<_, _, _, ()>(TBL_ACCOUNTS, &name, i + 1)
                .with_context(|| "failed to zadd")?;
            con.set::<_, _, ()>(format!("{}:{}", TBL_SAVINGS, i), saving_balance as i64)
                .with_context(|| "failed to set savings")?;
            con.set::<_, _, ()>(format!("{}:{}", TBL_CHECKING, i), check_balance as i64)
                .with_context(|| "failed to set check")?;

            if (i + 1) % 50000 == 0 {
                println!("prepared {}/{} accounts", i + 1, NUM_ACCOUNTS);
            }
        }
        accounts.push(name);
    }
    Ok(accounts)
}

fn smallbank_redlock(
    mut con: redis::Connection,
    rl: redlock::RedLock,
    names: &[String],
    dur: Duration,
    all_timer: Arc<Mutex<Timer>>,
) -> RedisResult<u64> {
    let start = Instant::now();
    let mut thpt = 0;
    let mut timer = Timer::new();
    while start.elapsed() < dur {
        let name0 = &names[rand::thread_rng().gen_range(0..names.len())];
        let name1 = {
            let mut name1 = &names[rand::thread_rng().gen_range(0..names.len())];
            while name1 == name0 {
                name1 = &names[rand::thread_rng().gen_range(0..names.len())];
            }
            name1
        };

        let op = rand::thread_rng().gen_range(0..100);
        let _timer_op = TimerOp::new(&mut timer);
        match op {
            x if x < 15 => rl_amalgamate(&mut con, &rl, name0, name1)?,
            x if x < 30 => rhl_balance(&mut con, name0).map(|_| ())?,
            x if x < 45 => rl_deposit_checking(&mut con, &rl, name0)?,
            x if x < 70 => rl_send_payment(&mut con, &rl, name0, name1)?,
            x if x < 85 => rl_transact_savings(&mut con, &rl, name0)?,
            _ => rl_write_check(&mut con, &rl, name0)?,
        }
        thpt += 1;
    }
    all_timer.lock().unwrap().merge(timer);
    Ok(thpt)
}

struct NetArgs {
    server: SocketAddr,
    dev: String,
}

#[tokio::main(flavor = "current_thread")]
async fn smallbank_handlock(
    mut con: redis::Connection,
    netargs: NetArgs,
    policy: HandlockAcquirePolicy,
    names: &[String],
    dur: Duration,
    all_timer: Arc<Mutex<Timer>>,
) -> RedisResult<u64> {
    const LOCAL_MEM_LEN: usize = 1 << 20;
    let (qp, remote) = utils::make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    let mut dci = utils::make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = (0..2)
        .map(|i| {
            let dct = utils::make_dct_together_with(&qp, true);
            Handlock::initiate_dct(&dct, mem.slice(i * LOCK_RESERVE, LOCK_RESERVE).unwrap())
                .expect("cannot post initial DCT recvs");
            dct
        })
        .collect::<Vec<_>>();
    let node_id = qp.port().unwrap().0.lid();

    let mut timer = Timer::new();
    let start = Instant::now();
    let mut thpt = 0;
    while start.elapsed() < dur {
        let meta = HandlockMeta {
            id: node_id,
            policy,
            qp: &qp,
            dci: &mut dci,
            dct0: &dct[0],
            dct1: &dct[1],
            local: mem.as_slice(),
            remote,
        };

        let name0 = &names[rand::thread_rng().gen_range(0..names.len())];
        let name1 = {
            let mut name1 = &names[rand::thread_rng().gen_range(0..names.len())];
            while name1 == name0 {
                name1 = &names[rand::thread_rng().gen_range(0..names.len())];
            }
            name1
        };

        let op = rand::thread_rng().gen_range(0..100);
        let _timer_op = TimerOp::new(&mut timer);
        match op {
            x if x < 15 => hl_amalgamate(&mut con, meta, name0, name1).await?,
            x if x < 30 => rhl_balance(&mut con, name0).map(|_| ())?,
            x if x < 45 => hl_deposit_checking(&mut con, meta, name0).await?,
            x if x < 70 => hl_send_payment(&mut con, meta, name0, name1).await?,
            x if x < 85 => hl_transact_savings(&mut con, meta, name0).await?,
            _ => hl_write_check(&mut con, meta, name0).await?,
        }
        thpt += 1;
    }
    all_timer.lock().unwrap().merge(timer);
    Ok(thpt)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let redis_endpoint = format!("redis://{}", args.redis);
    let cli =
        redis::Client::open(redis_endpoint.as_str()).with_context(|| "failed to open client")?;

    let names = {
        let mut con = cli
            .get_connection()
            .with_context(|| "failed to get connection")?;
        println!("start preparing accounts");
        let names = Arc::new(prepare(&mut con, args.prepare).with_context(|| "failed to prepare")?);
        if args.prepare {
            println!("prepared {} accounts", names.len());
            return Ok(());
        }
        names
    };

    let lockty = args.lock;
    let duration = args.duration;
    println!("running for {} seconds", duration);

    let timer = Arc::new(Mutex::new(Timer::new()));
    let mut handles = Vec::with_capacity(args.nthreads);

    let start = Instant::now();
    for i in 0..args.nthreads {
        let con = cli.get_connection()?;
        let rl = redlock::RedLock::new(vec![redis_endpoint.as_str()]);

        let timer = timer.clone();
        let names = names.clone();
        let netargs = NetArgs {
            server: args.server,
            dev: args.dev.clone(),
        };
        let handle = thread::spawn(move || {
            core_affinity::set_for_current(core_affinity::CoreId { id: i });
            match lockty {
                LockType::RedLock => {
                    smallbank_redlock(con, rl, &names, Duration::from_secs(duration), timer)
                }
                LockType::Handlock => {
                    let policy = HandlockAcquirePolicy {
                        remote_wait: match args.backoff {
                            0 => None,
                            _ => Some(args.backoff),
                        },
                    };
                    smallbank_handlock(
                        con,
                        netargs,
                        policy,
                        &names,
                        Duration::from_secs(duration),
                        timer,
                    )
                }
            }
        });
        handles.push(handle);
    }

    let tot = handles
        .into_iter()
        .map(|t| t.join().unwrap().unwrap())
        .sum::<u64>();
    let duration = start.elapsed().as_secs_f64();
    let thpt = tot as f64 / duration;

    println!("throughput: {:.2} tx/s", thpt);

    if let Some(output) = args.output {
        {
            use std::io::Write;
            let mut out = std::fs::File::create(&output)?;
            writeln!(out, "throughput: {:.2} tx/s", thpt)?;
            writeln!(out)?;
        }

        let mut timer = timer.lock().unwrap();
        timer.report().append_to_file(&output, duration)?;
    }

    Ok(())
}
