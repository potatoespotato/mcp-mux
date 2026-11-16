//! Shared identity and filesystem layout for the shim, the daemon and the
//! Python CLI (`mcpmux status|stop`). All three must agree on these paths.

pub mod sha256;

use std::env;
use std::fs;
use std::path::PathBuf;

extern "C" {
    #[link_name = "getuid"]
    fn libc_getuid() -> u32;
}

pub fn home() -> PathBuf {
    PathBuf::from(env::var("HOME").unwrap_or_else(|_| "/root".into()))
}

pub fn runtime_dir() -> PathBuf {
    let base = env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/tmp/mcpmux-{}", unsafe { libc_getuid() }));
    let dir = PathBuf::from(base).join("mcpmux");
    let _ = fs::create_dir_all(&dir);
    dir
}

pub fn log_dir() -> PathBuf {
    let base = env::var("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home().join(".local/state"));
    let dir = base.join("mcpmux");
    let _ = fs::create_dir_all(&dir);
    dir
}

/// Daemon identity: the name, plus for cwd-scoped backends the first 12 hex
/// chars of sha256(canonical cwd), so only sessions on the same root share.
pub fn resolve_key(name: &str, cwd_scoped: bool) -> String {
    if !cwd_scoped {
        return name.to_string();
    }
    let cwd = env::current_dir()
        .and_then(|p| p.canonicalize())
        .unwrap_or_else(|_| PathBuf::from("."));
    let digest = sha256::hex(cwd.to_string_lossy().as_bytes());
    format!("{}-{}", name, &digest[..12])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proto {
    Mcp,
    Lsp,
}

impl Proto {
    pub fn parse(s: &str) -> Option<Proto> {
        match s {
            "mcp" => Some(Proto::Mcp),
            "lsp" => Some(Proto::Lsp),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Proto::Mcp => "mcp",
            Proto::Lsp => "lsp",
        }
    }
}

pub struct Paths {
    pub key: String,
    pub sock: PathBuf,
    pub lock: PathBuf,
    pub pid: PathBuf,
    pub meta: PathBuf,
    pub log: PathBuf,
}

impl Paths {
    pub fn new(key: String) -> Self {
        let rt = runtime_dir();
        Paths {
            sock: rt.join(format!("{}.sock", key)),
            lock: rt.join(format!("{}.lock", key)),
            pid: rt.join(format!("{}.pid", key)),
            meta: rt.join(format!("{}.json", key)),
            log: log_dir().join(format!("{}.log", key)),
            key,
        }
    }
}
