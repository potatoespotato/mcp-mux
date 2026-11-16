//! mcpmuxd - owns one stdio backend (MCP or LSP server) and fans many shim
//! connections onto its single stdin/stdout pair.
//!
//! Started on demand by the first `mcpmux-shim`, outlives it so the backend
//! stays warm, exits after `--idle` seconds without clients (0 = never) or
//! when the backend dies. Pid/meta files feed `mcpmux status`.

mod framing;
mod hub;
mod init;
mod lsp;
mod mcp;

use framing::Framing;
use hub::{Event, Io, Protocol, Sid};
use mcpmux::{Paths, Proto};
use serde_json::json;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::{Child, Command};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

const READ_BUF: usize = 256 * 1024;
const TICK: Duration = Duration::from_secs(5);
const TERM_GRACE: Duration = Duration::from_secs(5);

#[repr(C)]
struct Tm {
    sec: i32,
    min: i32,
    hour: i32,
    mday: i32,
    mon: i32,
    year: i32,
    wday: i32,
    yday: i32,
    isdst: i32,
    gmtoff: i64,
    zone: *const u8,
}

extern "C" {
    fn localtime_r(t: *const i64, out: *mut Tm) -> *mut Tm;
    fn kill(pid: i32, sig: i32) -> i32;
}

const SIGTERM: i32 = 15;

pub fn log(msg: &str) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let mut tm: Tm = unsafe { std::mem::zeroed() };
    unsafe { localtime_r(&now, &mut tm) };
    eprintln!("[{:02}:{:02}:{:02}] {}", tm.hour, tm.min, tm.sec, msg);
}

struct Args {
    key: String,
    proto: Proto,
    idle: f64,
    argv: Vec<String>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let (mut key, mut proto, mut idle) = (None, Proto::Mcp, 0.0);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => key = args.next(),
            "--proto" => {
                proto = args.next().and_then(|v| Proto::parse(&v)).unwrap_or_else(|| usage())
            }
            "--idle" => idle = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| usage()),
            "--" => break,
            other => {
                eprintln!("mcpmuxd: unexpected argument {:?}", other);
                usage();
            }
        }
    }
    let argv: Vec<String> = args.collect();
    match key {
        Some(key) if !key.is_empty() && !argv.is_empty() => Args { key, proto, idle, argv },
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!("usage: mcpmuxd --name KEY [--proto mcp|lsp] [--idle SECONDS] -- CMD [ARGS...]");
    std::process::exit(2);
}

struct Meta<'a> {
    paths: &'a Paths,
    base: serde_json::Value,
}

impl Meta<'_> {
    fn publish(&self, clients: usize, peak: usize) {
        let mut meta = self.base.clone();
        meta["clients"] = clients.into();
        meta["peak_clients"] = peak.into();
        let _ = fs::write(&self.paths.meta, meta.to_string());
    }
}

fn spawn_backend(argv: &[String]) -> std::io::Result<Child> {
    Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| std::io::Error::new(e.kind(), format!("cannot start backend {:?}: {}", argv[0], e)))
}

/// Backend stdout -> hub, hub -> backend stdin.
fn wire_backend(child: &mut Child, framing: Framing, events: UnboundedSender<Event>) -> UnboundedSender<Vec<u8>> {
    let mut stdin = child.stdin.take().expect("piped");
    let stdout = child.stdout.take().expect("piped");
    let (tx, mut rx) = unbounded_channel::<Vec<u8>>();

    tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if stdin.write_all(&bytes).await.is_err() || stdin.flush().await.is_err() {
                log("backend stdin closed");
                break;
            }
        }
    });

    tokio::spawn(async move {
        let mut reader = BufReader::with_capacity(READ_BUF, stdout);
        loop {
            match framing.read(&mut reader, "backend").await {
                Ok(Some(msg)) => {
                    if events.send(Event::Backend(msg)).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    log(&format!("backend read error: {}", e));
                    break;
                }
            }
        }
        let _ = events.send(Event::BackendEof);
    });

    tx
}

/// One task pair per shim connection; the hub only sees decoded messages.
fn accept_loop(listener: UnixListener, framing: Framing, events: UnboundedSender<Event>) {
    tokio::spawn(async move {
        let mut next: Sid = 0;
        loop {
            let Ok((stream, _)) = listener.accept().await else { continue };
            next += 1;
            let sid = next;
            let (rd, mut wr) = stream.into_split();
            let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
            if events.send(Event::Attach(sid, tx)).is_err() {
                return;
            }
            tokio::spawn(async move {
                while let Some(bytes) = rx.recv().await {
                    if wr.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                // Dropping `wr` shuts down the write half: the shim sees EOF.
            });
            let events = events.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::with_capacity(READ_BUF, rd);
                let who = format!("session {}", sid);
                loop {
                    match framing.read(&mut reader, &who).await {
                        Ok(Some(msg)) => {
                            if events.send(Event::Client(sid, msg)).is_err() {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            log(&format!("{}: read ended ({})", who, e));
                            break;
                        }
                    }
                }
                let _ = events.send(Event::Detach(sid));
            });
        }
    });
}

async fn terminate(child: &mut Child) {
    if let Ok(Some(_)) = child.try_wait() {
        return;
    }
    if let Some(pid) = child.id() {
        unsafe { kill(pid as i32, SIGTERM) };
    }
    if tokio::time::timeout(TERM_GRACE, child.wait()).await.is_err() {
        let _ = child.kill().await;
    }
}

/// Remove our runtime files, unless a newer daemon with the same key already
/// took them over (stale socket replaced by a shim while we were exiting).
fn cleanup(paths: &Paths, sock_ino: u64) {
    if fs::metadata(&paths.sock).map(|m| m.ino()).ok() == Some(sock_ino) {
        let _ = fs::remove_file(&paths.sock);
    }
    let ours = fs::read_to_string(&paths.pid)
        .map(|p| p.trim() == std::process::id().to_string())
        .unwrap_or(false);
    if ours {
        let _ = fs::remove_file(&paths.pid);
        let _ = fs::remove_file(&paths.meta);
    }
}

async fn run(args: Args) -> std::io::Result<()> {
    let paths = Paths::new(args.key.clone());
    let framing = match args.proto {
        Proto::Mcp => Framing::Lines,
        Proto::Lsp => Framing::Lsp,
    };
    let mut proto: Box<dyn Protocol> = match args.proto {
        Proto::Mcp => Box::new(mcp::Mcp::new()),
        Proto::Lsp => Box::new(lsp::Lsp::new()),
    };

    let (events_tx, mut events) = unbounded_channel::<Event>();
    let mut child = spawn_backend(&args.argv)?;
    log(&format!(
        "backend pid={} proto={}: {}",
        child.id().unwrap_or(0),
        args.proto.as_str(),
        args.argv.join(" ")
    ));
    let backend_tx = wire_backend(&mut child, framing, events_tx.clone());
    let mut io = Io::new(framing, backend_tx);

    let _ = fs::remove_file(&paths.sock);
    let listener = UnixListener::bind(&paths.sock)?;
    let sock_ino = fs::metadata(&paths.sock)?.ino();
    fs::write(&paths.pid, std::process::id().to_string())?;
    let meta = Meta {
        paths: &paths,
        base: json!({
            "key": paths.key,
            "proto": args.proto.as_str(),
            "argv": args.argv,
            "cwd": std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
            "started": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64(),
            "idle": args.idle,
        }),
    };
    meta.publish(0, 0);
    log(&format!("listening on {}", paths.sock.display()));
    accept_loop(listener, framing, events_tx);

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut tick = tokio::time::interval(TICK);
    let mut attached: std::collections::BTreeSet<Sid> = Default::default();
    let mut peak = 0usize;
    let mut idle_since = Some(Instant::now());

    loop {
        tokio::select! {
            ev = events.recv() => match ev {
                Some(Event::Attach(sid, tx)) => {
                    io.add(sid, tx);
                    attached.insert(sid);
                    peak = peak.max(attached.len());
                    idle_since = None;
                    meta.publish(attached.len(), peak);
                    log(&format!("session {} attached ({} live)", sid, attached.len()));
                }
                Some(Event::Client(sid, msg)) => proto.client(&mut io, sid, msg),
                Some(Event::Detach(sid)) => {
                    if attached.remove(&sid) {
                        io.close(sid);
                        proto.detach(&mut io, sid);
                        if attached.is_empty() {
                            idle_since = Some(Instant::now());
                        }
                        meta.publish(attached.len(), peak);
                        log(&format!("session {} detached ({} live)", sid, attached.len()));
                    }
                }
                Some(Event::Backend(msg)) => proto.backend(&mut io, msg),
                Some(Event::BackendEof) | None => {
                    let rc = child.wait().await.ok().and_then(|s| s.code());
                    log(&format!("backend exited rc={:?}; dropping {} session(s)", rc, attached.len()));
                    break;
                }
            },
            _ = tick.tick() => {
                proto.tick(&mut io);
                if args.idle > 0.0 {
                    if let Some(since) = idle_since {
                        if since.elapsed().as_secs_f64() > args.idle {
                            log(&format!("idle {}s with no clients, shutting down", args.idle));
                            break;
                        }
                    }
                }
            },
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }

    io.close_all();
    cleanup(&paths, sock_ino);
    terminate(&mut child).await;
    log("daemon exit");
    Ok(())
}

fn main() {
    let args = parse_args();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(e) = rt.block_on(run(args)) {
        log(&format!("fatal: {}", e));
        std::process::exit(1);
    }
}
