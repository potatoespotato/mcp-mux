//! MCP routing. Port of the original Python daemon:
//!   - `initialize` served from cache, `notifications/initialized` sent once
//!   - request ids and progress tokens rewritten, replies mapped back
//!   - server-originated requests (sampling/roots/elicitation) go to one client
//!   - server notifications are broadcast

use crate::hub::{has_id, key, method, with_id, Io, Protocol, Sid};
use crate::init::{InitGate, InitHooks};
use crate::log;
use serde_json::{json, Value};
use std::collections::HashMap;

pub struct Mcp {
    init: InitGate,
    initialized_sent: bool,
    /// backend id -> (client, client's id)
    calls: HashMap<i64, (Sid, Value)>,
    /// our progress token -> (client, client's token)
    tokens: HashMap<String, (Sid, Value)>,
    /// our id for a server request -> (client it went to, server's id)
    server_calls: HashMap<String, (Sid, Value)>,
}

impl Mcp {
    pub fn new() -> Self {
        Mcp {
            init: InitGate::new(InitHooks { prepare: |_| {}, present: |v| v }),
            initialized_sent: false,
            calls: HashMap::new(),
            tokens: HashMap::new(),
            server_calls: HashMap::new(),
        }
    }

    fn initialize(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        if let Some(cached) = self.init.cached() {
            let asked = msg.pointer("/params/protocolVersion").and_then(Value::as_str);
            let served = cached.get("protocolVersion").and_then(Value::as_str);
            if let (Some(a), Some(s)) = (asked, served) {
                if a != s {
                    log(&format!("protocolVersion mismatch: client {}, backend {}", a, s));
                }
            }
        }
        self.init.request(io, sid, msg);
    }

    fn cancel(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let Some(original) = msg.pointer("/params/requestId") else { return };
        let bid = self
            .calls
            .iter()
            .find(|(_, (owner, oid))| *owner == sid && oid == original)
            .map(|(bid, _)| *bid);
        let Some(bid) = bid else { return };
        self.calls.remove(&bid);
        let mut out = msg.clone();
        out["params"]["requestId"] = bid.into();
        io.to_backend(&out);
    }

    fn forward_request(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let bid = io.new_id();
        self.calls.insert(bid, (sid, msg["id"].clone()));
        let mut out = with_id(&msg, bid);
        if let Some(token) = msg.pointer("/params/_meta/progressToken") {
            let ours = format!("mux-{}", bid);
            self.tokens.insert(ours.clone(), (sid, token.clone()));
            out["params"]["_meta"]["progressToken"] = ours.into();
        }
        io.to_backend(&out);
    }
}

impl Protocol for Mcp {
    fn client(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        match method(&msg) {
            Some("initialize") => self.initialize(io, sid, msg),
            Some("notifications/initialized") => {
                // Only the real handshake gets one; later clients are served cache.
                if !self.initialized_sent {
                    self.initialized_sent = true;
                    io.to_backend(&msg);
                }
            }
            Some("notifications/cancelled") => self.cancel(io, sid, msg),
            Some(_) if has_id(&msg) => self.forward_request(io, sid, msg),
            Some(_) => io.to_backend(&msg),
            None if has_id(&msg) => {
                // A reply to a server-originated request.
                if let Some((_, server_id)) = self.server_calls.remove(&key(&msg["id"])) {
                    io.to_backend(&with_id(&msg, server_id));
                }
            }
            None => io.to_backend(&msg),
        }
    }

    fn detach(&mut self, io: &mut Io, sid: Sid) {
        self.init.detach(sid);
        let owned: Vec<i64> =
            self.calls.iter().filter(|(_, (o, _))| *o == sid).map(|(b, _)| *b).collect();
        for bid in owned {
            self.calls.remove(&bid);
            io.to_backend(&json!({
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": bid, "reason": "client disconnected"},
            }));
        }
        self.tokens.retain(|_, (o, _)| *o != sid);
        self.server_calls.retain(|_, (o, _)| *o != sid);
    }

    fn backend(&mut self, io: &mut Io, msg: Value) {
        match method(&msg) {
            Some("notifications/progress") => {
                let Some(token) = msg.pointer("/params/progressToken").and_then(Value::as_str)
                else {
                    return;
                };
                if let Some((sid, original)) = self.tokens.get(token) {
                    let mut out = msg.clone();
                    out["params"]["progressToken"] = original.clone();
                    io.send(*sid, &out);
                }
            }
            Some(m) if has_id(&msg) => {
                let Some(sid) = io.first_session() else {
                    log(&format!("no client for server request {}, dropped", m));
                    return;
                };
                let cid = format!("mux-srv-{}", io.new_id());
                self.server_calls.insert(key(&Value::from(cid.clone())), (sid, msg["id"].clone()));
                io.send(sid, &with_id(&msg, cid));
            }
            Some(_) => io.broadcast(&msg),
            None if self.init.is_reply(&msg) => self.init.reply(io, msg),
            None => {
                let Some(bid) = msg.get("id").and_then(Value::as_i64) else { return };
                let Some((sid, original)) = self.calls.remove(&bid) else { return };
                self.tokens.remove(&format!("mux-{}", bid));
                io.send(sid, &with_id(&msg, original));
            }
        }
    }

    fn tick(&mut self, io: &mut Io) {
        self.init.tick(io);
    }
}
