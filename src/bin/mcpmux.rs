//! mcpmux - management CLI: list running daemons with their memory, stop them.

use mcpmux::runtime_dir;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

const SIGTERM: i32 = 15;

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

const USAGE: &str = "usage:\n  mcpmux status\n  mcpmux stop NAME|--all";

fn rss_mib(pid: u32) -> f64 {
    fs::read_to_string(format!("/proc/{}/status", pid))
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<f64>().ok())
        })
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

/// Every process below `pid` (wrapper -> real server -> helpers).
fn descendants(pid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let mut stack = vec![pid];
    while let Some(current) = stack.pop() {
        let Ok(tasks) = fs::read_dir(format!("/proc/{}/task", current)) else { continue };
        for task in tasks.flatten() {
            let Ok(kids) = fs::read_to_string(task.path().join("children")) else { continue };
            for kid in kids.split_whitespace().filter_map(|k| k.parse().ok()) {
                out.push(kid);
                stack.push(kid);
            }
        }
    }
    out
}

/// Live daemons as (key, pid); pid files of dead daemons are cleaned up.
fn live_daemons() -> Vec<(String, u32, PathBuf)> {
    let dir = runtime_dir();
    let mut live = Vec::new();
    let Ok(entries) = fs::read_dir(&dir) else { return live };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("pid") {
            continue;
        }
        let Some(pid) = fs::read_to_string(&path).ok().and_then(|s| s.trim().parse::<u32>().ok())
        else {
            continue;
        };
        if !Path::new(&format!("/proc/{}", pid)).exists() {
            for ext in ["pid", "sock", "json"] {
                let _ = fs::remove_file(path.with_extension(ext));
            }
            continue;
        }
        let key = path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
        live.push((key, pid, path));
    }
    live.sort();
    live
}

fn status() -> i32 {
    let live = live_daemons();
    if live.is_empty() {
        println!("mcpmux: no daemons running");
        return 0;
    }
    println!("{:<30}{:>6}{:>8}{:>7}{:>8}{:>9}  CMD", "KEY", "PROTO", "PID", "SHIM", "DAEMON", "BACKEND");
    let mut total = 0.0;
    for (key, pid, path) in live {
        let daemon = rss_mib(pid);
        let backend: f64 = descendants(pid).into_iter().map(rss_mib).sum();
        total += daemon + backend;
        let meta: Value = fs::read_to_string(path.with_extension("json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Value::Null);
        let proto = meta.get("proto").and_then(Value::as_str).unwrap_or("mcp");
        let clients = meta.get("clients").and_then(Value::as_u64).unwrap_or(0);
        let cmd: String = meta
            .get("argv")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|| "?".into());
        let cmd: String = cmd.chars().take(52).collect();
        println!(
            "{:<30}{:>6}{:>8}{:>7}{:>7.0}M{:>8.0}M  {}",
            key, proto, pid, clients, daemon, backend, cmd
        );
    }
    println!("\nresident total (daemons + backends): {:.0} MiB", total);
    0
}

fn stop(name: Option<&str>, all: bool) -> i32 {
    let targets: Vec<_> = live_daemons()
        .into_iter()
        .filter(|(key, _, _)| all || name.is_some_and(|n| key.starts_with(n)))
        .collect();
    if targets.is_empty() {
        println!("mcpmux: nothing to stop");
        return 1;
    }
    for (key, pid, _) in targets {
        if unsafe { kill(pid as i32, SIGTERM) } == 0 {
            println!("stopped {} (pid {})", key, pid);
        } else {
            println!("{}: {}", key, std::io::Error::last_os_error());
        }
    }
    0
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("status") => status(),
        Some("stop") => {
            let rest = &args[1..];
            let all = rest.iter().any(|a| a == "--all");
            let name = rest.iter().find(|a| !a.starts_with('-')).map(String::as_str);
            if !all && name.is_none() {
                eprintln!("{}", USAGE);
                2
            } else {
                stop(name, all)
            }
        }
        _ => {
            eprintln!("{}", USAGE);
            2
        }
    };
    std::process::exit(code);
}
