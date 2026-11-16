//! The single owner of all routing state. Socket readers, writers and the
//! backend pipe only move bytes; every decision happens here, on one task,
//! so protocol state needs no locks.

use crate::framing::Framing;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::sync::mpsc::UnboundedSender;

pub type Sid = u64;

pub enum Event {
    Attach(Sid, UnboundedSender<Vec<u8>>),
    Client(Sid, Value),
    Detach(Sid),
    Backend(Value),
    BackendEof,
}

/// Outbound side of the hub, handed to protocol handlers.
pub struct Io {
    framing: Framing,
    sessions: BTreeMap<Sid, UnboundedSender<Vec<u8>>>,
    backend: UnboundedSender<Vec<u8>>,
    counter: i64,
}

impl Io {
    pub fn new(framing: Framing, backend: UnboundedSender<Vec<u8>>) -> Self {
        Io { framing, sessions: BTreeMap::new(), backend, counter: 0 }
    }

    pub fn new_id(&mut self) -> i64 {
        self.counter += 1;
        self.counter
    }

    pub fn add(&mut self, sid: Sid, tx: UnboundedSender<Vec<u8>>) {
        self.sessions.insert(sid, tx);
    }

    /// Drop the writer: the shim sees EOF and exits.
    pub fn close(&mut self, sid: Sid) {
        self.sessions.remove(&sid);
    }

    pub fn close_all(&mut self) {
        self.sessions.clear();
    }

    pub fn send(&self, sid: Sid, msg: &Value) {
        if let Some(tx) = self.sessions.get(&sid) {
            let _ = tx.send(self.framing.encode(msg));
        }
    }

    pub fn broadcast(&self, msg: &Value) {
        let bytes = self.framing.encode(msg);
        for tx in self.sessions.values() {
            let _ = tx.send(bytes.clone());
        }
    }

    pub fn to_backend(&self, msg: &Value) {
        let _ = self.backend.send(self.framing.encode(msg));
    }

    /// Oldest live session; the one server-originated requests go to.
    pub fn first_session(&self) -> Option<Sid> {
        self.sessions.keys().next().copied()
    }
}

pub trait Protocol {
    fn client(&mut self, io: &mut Io, sid: Sid, msg: Value);
    fn detach(&mut self, io: &mut Io, sid: Sid);
    fn backend(&mut self, io: &mut Io, msg: Value);
    /// Called every few seconds for timeouts.
    fn tick(&mut self, io: &mut Io);
}

// ---------------------------------------------------------------------------
// helpers shared by both protocols
// ---------------------------------------------------------------------------

pub fn method(msg: &Value) -> Option<&str> {
    msg.get("method").and_then(Value::as_str)
}

pub fn has_id(msg: &Value) -> bool {
    msg.get("id").is_some()
}

pub fn with_id(msg: &Value, id: impl Into<Value>) -> Value {
    let mut out = msg.clone();
    out["id"] = id.into();
    out
}

pub fn result(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub fn error(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// Map key for an id/token of any JSON type (string or number).
pub fn key(v: &Value) -> String {
    v.to_string()
}
