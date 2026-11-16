//! mcpmux-shim - the per-session half of mcpmux.
//!
//! Claude Code launches this; it connects to the daemon (`mcpmuxd`) that owns
//! the real MCP or LSP backend and pipes stdio to it, spawning that daemon on
//! first use. A plain byte pipe: the protocol only matters to the daemon. Kept
//! free of async because one of these runs per session per server.

use mcpmux::{resolve_key, Paths, Proto};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const BUF: usize = 256 * 1024;
const SPAWN_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);
const LOCK_EX: i32 = 2;

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
    fn setsid() -> i32;
}

struct Opts {
    proto: Proto,
    idle: f64,
    argv: Vec<String>,
}

fn try_connect(paths: &Paths) -> Option<UnixStream> {
    UnixStream::connect(&paths.sock).ok()
}

/// The daemon binary lives next to this shim (both install into ~/.local/bin).
fn daemon_path() -> std::io::Result<PathBuf> {
    let exe = env::current_exe()?;
    let path = exe.with_file_name("mcpmuxd");
    if path.exists() {
        Ok(path)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("daemon not found at {}", path.display()),
        ))
    }
}

fn spawn_daemon(paths: &Paths, opts: &Opts) -> std::io::Result<std::process::Child> {
    let daemon = daemon_path()?;
    let log = OpenOptions::new().create(true).append(true).open(&paths.log)?;
    let err = log.try_clone()?;

    let mut cmd = Command::new(daemon);
    cmd.arg("--name")
        .arg(&paths.key)
        .arg("--proto")
        .arg(opts.proto.as_str())
        .arg("--idle")
        .arg(opts.idle.to_string())
        .arg("--")
        .args(&opts.argv)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));

    // Detach so the daemon outlives this shim and keeps the backend warm.
    unsafe {
        cmd.pre_exec(|| {
            setsid();
            Ok(())
        });
    }
    cmd.spawn()
}

fn connect_or_spawn(paths: &Paths, opts: &Opts) -> UnixStream {
    if let Some(stream) = try_connect(paths) {
        return stream;
    }

    // Serialise startup; a concurrent shim may already be spawning the daemon.
    let lock = File::create(&paths.lock).unwrap_or_else(|e| {
        die(&format!("cannot open lock {}: {}", paths.lock.display(), e))
    });
    unsafe {
        flock(lock.as_raw_fd(), LOCK_EX);
    }

    if let Some(stream) = try_connect(paths) {
        return stream;
    }
    let _ = fs::remove_file(&paths.sock); // stale socket from a dead daemon
    let mut daemon = spawn_daemon(paths, opts)
        .unwrap_or_else(|e| die(&format!("cannot spawn daemon: {}", e)));

    let deadline = Instant::now() + SPAWN_TIMEOUT;
    while Instant::now() < deadline {
        if let Some(stream) = try_connect(paths) {
            return stream;
        }
        // A daemon that died before listening (e.g. backend not found) will
        // never come up; fail now instead of waiting out the timeout.
        if let Ok(Some(status)) = daemon.try_wait() {
            die(&format!(
                "daemon '{}' exited ({}); see {}",
                paths.key,
                status,
                paths.log.display()
            ));
        }
        thread::sleep(POLL);
    }
    die(&format!(
        "daemon '{}' did not come up; see {}",
        paths.key,
        paths.log.display()
    ));
}

fn die(msg: &str) -> ! {
    eprintln!("mcpmux-shim: {}", msg);
    std::process::exit(1);
}

/// stdin -> socket in a thread, socket -> stdout on the main thread.
fn pipe(stream: UnixStream) -> std::io::Result<()> {
    let mut to_daemon = stream.try_clone()?;
    let up = thread::spawn(move || {
        let mut stdin = unsafe { File::from_raw_fd(0) };
        let mut buf = vec![0u8; BUF];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if to_daemon.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        // Half-close so the daemon sees EOF while replies can still arrive.
        let _ = to_daemon.shutdown(std::net::Shutdown::Write);
    });

    let mut from_daemon = stream;
    let mut stdout = unsafe { File::from_raw_fd(1) };
    let mut buf = vec![0u8; BUF];
    loop {
        match from_daemon.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if stdout.write_all(&buf[..n]).is_err() {
                    break;
                }
                let _ = stdout.flush();
            }
        }
    }
    drop(up);
    Ok(())
}

fn usage() -> ! {
    eprintln!(
        "usage: mcpmux-shim --name NAME [--proto mcp|lsp] [--cwd-scoped] [--idle SECONDS] -- CMD [ARGS...]"
    );
    std::process::exit(2);
}

fn main() {
    let mut args = env::args().skip(1);
    let mut name: Option<String> = None;
    let mut cwd_scoped = false;
    let mut opts = Opts { proto: Proto::Mcp, idle: 0.0, argv: Vec::new() };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => name = args.next(),
            "--cwd-scoped" => cwd_scoped = true,
            "--proto" => {
                opts.proto = args
                    .next()
                    .and_then(|v| Proto::parse(&v))
                    .unwrap_or_else(|| usage());
            }
            "--idle" => {
                opts.idle = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--" => {
                opts.argv.extend(args);
                break;
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("mcpmux-shim: unexpected argument {:?}", other);
                usage();
            }
        }
    }

    let name = match name {
        Some(n) if !n.is_empty() => n,
        _ => usage(),
    };
    if opts.argv.is_empty() {
        usage();
    }

    let paths = Paths::new(resolve_key(&name, cwd_scoped));
    let stream = connect_or_spawn(&paths, &opts);
    if let Err(e) = pipe(stream) {
        die(&format!("pipe failed: {}", e));
    }
}
