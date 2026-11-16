//! `initialize` gate, shared by MCP and LSP: only the first handshake reaches
//! the backend; its result is cached and served to every later client.
//! Clients arriving while the handshake is in flight queue up behind it.

use crate::hub::{error, result, with_id, Io, Sid};
use crate::log;
use serde_json::Value;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const INIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Protocol-specific edits: on the request going to the backend, and on the
/// result before clients see it.
pub struct InitHooks {
    pub prepare: fn(&mut Value),
    pub present: fn(Value) -> Value,
}

struct Flight {
    bid: i64,
    sid: Sid,
    id: Value,
    started: Instant,
}

pub struct InitGate {
    hooks: InitHooks,
    cached: Option<Value>,
    flight: Option<Flight>,
    waiters: VecDeque<(Sid, Value)>,
}

impl InitGate {
    pub fn new(hooks: InitHooks) -> Self {
        InitGate { hooks, cached: None, flight: None, waiters: VecDeque::new() }
    }

    pub fn cached(&self) -> Option<&Value> {
        self.cached.as_ref()
    }

    pub fn request(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        if let Some(res) = &self.cached {
            io.send(sid, &result(&msg["id"], res.clone()));
        } else if self.flight.is_some() {
            self.waiters.push_back((sid, msg));
        } else {
            self.start(io, sid, msg);
        }
    }

    fn start(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let bid = io.new_id();
        let mut out = with_id(&msg, bid);
        (self.hooks.prepare)(&mut out);
        io.to_backend(&out);
        self.flight = Some(Flight { bid, sid, id: msg["id"].clone(), started: Instant::now() });
    }

    /// Next queued client retries the handshake after a failure.
    fn next(&mut self, io: &mut Io) {
        if let Some((sid, msg)) = self.waiters.pop_front() {
            self.start(io, sid, msg);
        }
    }

    pub fn is_reply(&self, msg: &Value) -> bool {
        match (&self.flight, msg.get("id").and_then(Value::as_i64)) {
            (Some(f), Some(id)) => f.bid == id,
            _ => false,
        }
    }

    pub fn reply(&mut self, io: &mut Io, msg: Value) {
        let Some(flight) = self.flight.take() else { return };
        match msg.get("result") {
            Some(res) => {
                let res = (self.hooks.present)(res.clone());
                io.send(flight.sid, &result(&flight.id, res.clone()));
                for (sid, waiting) in self.waiters.drain(..) {
                    io.send(sid, &result(&waiting["id"], res.clone()));
                }
                self.cached = Some(res);
            }
            None => {
                log("backend rejected initialize");
                io.send(flight.sid, &with_id(&msg, flight.id));
                self.next(io);
            }
        }
    }

    pub fn tick(&mut self, io: &mut Io) {
        let expired = matches!(&self.flight, Some(f) if f.started.elapsed() > INIT_TIMEOUT);
        if !expired {
            return;
        }
        let flight = self.flight.take().expect("checked above");
        log("backend initialize timed out");
        io.send(flight.sid, &error(&flight.id, -32001, "mcpmux: backend initialize timed out"));
        self.next(io);
    }

    pub fn detach(&mut self, sid: Sid) {
        self.waiters.retain(|(s, _)| *s != sid);
    }
}
