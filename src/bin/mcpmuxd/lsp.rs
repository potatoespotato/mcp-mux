//! LSP routing. One language server, many clients on the same workspace root.
//!
//! What makes this harder than MCP is shared state on the server side:
//!   - lifecycle: `initialize` is cached, `initialized` sent once, a client's
//!     `shutdown`/`exit` only ends that client; `processId` is replaced with
//!     ours so the server does not exit with the first client.
//!   - documents: the server holds one copy per URI. The daemon counts which
//!     clients have it open, forwards the first `didOpen` and the last
//!     `didClose`, turns later opens into a full-text `didChange`, and owns
//!     the version counter. Clients are told the server wants full sync, so
//!     no client ever sends a range edit against text another client changed.
//!   - diagnostics go only to clients that have the URI open; the last set is
//!     cached and replayed to a client that opens the URI later.
//!   - progress tokens and request ids are rewritten per client.
//!   - pull->push bridge: if the server only offers pull diagnostics
//!     (`diagnosticProvider`) and a client can only take push, the daemon
//!     pulls after every change and publishes the result itself.

use crate::hub::{error, has_id, key, method, result, with_id, Io, Protocol, Sid};
use crate::init::{InitGate, InitHooks};
use crate::log;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet};

const REQUEST_CANCELLED: i64 = -32800;
const INTERNAL_ERROR: i64 = -32603;
const SYNC_FULL: i64 = 1;
const SYNC_INCREMENTAL: i64 = 2;

struct Doc {
    openers: BTreeSet<Sid>,
    version: i64,
}

pub struct Lsp {
    init: InitGate,
    initialized_sent: bool,
    /// backend id -> (client, client's id)
    calls: HashMap<i64, (Sid, Value)>,
    /// our id for a server request -> (client it went to, server's id)
    server_calls: HashMap<String, (Sid, Value)>,
    /// our token -> (client, client's token), for workDone/partialResult
    tokens: HashMap<String, (Sid, Value)>,
    /// tokens the server created via window/workDoneProgress/create; the
    /// daemon accepted them itself, their progress goes nowhere
    own_tokens: HashSet<String>,
    docs: HashMap<String, Doc>,
    diagnostics: HashMap<String, Value>,
    /// clients that declared pull diagnostics support themselves
    client_pulls: HashSet<Sid>,
    /// bridge pulls in flight: backend id -> (uri, doc version asked for)
    pulls: HashMap<i64, (String, i64)>,
}

fn prepare_initialize(msg: &mut Value) {
    // Servers exit when the process in `processId` dies. That must be the
    // daemon, not whichever client happened to connect first.
    msg["params"]["processId"] = std::process::id().into();
}

fn present_initialize(mut res: Value) -> Value {
    if let Some(sync) = res.pointer_mut("/capabilities/textDocumentSync") {
        if sync.as_i64() == Some(SYNC_INCREMENTAL) {
            *sync = SYNC_FULL.into();
        } else if sync.get("change").and_then(Value::as_i64) == Some(SYNC_INCREMENTAL) {
            sync["change"] = SYNC_FULL.into();
        }
    }
    res
}

fn uri_of(msg: &Value) -> Option<String> {
    msg.pointer("/params/textDocument/uri").and_then(Value::as_str).map(str::to_owned)
}

impl Lsp {
    pub fn new() -> Self {
        Lsp {
            init: InitGate::new(InitHooks {
                prepare: prepare_initialize,
                present: present_initialize,
            }),
            initialized_sent: false,
            calls: HashMap::new(),
            server_calls: HashMap::new(),
            tokens: HashMap::new(),
            own_tokens: HashSet::new(),
            docs: HashMap::new(),
            diagnostics: HashMap::new(),
            client_pulls: HashSet::new(),
            pulls: HashMap::new(),
        }
    }

    // -- pull -> push bridge ----------------------------------------------

    /// `Some(inter_file)` when the server serves pull diagnostics.
    fn backend_pull(&self) -> Option<bool> {
        let provider = self.init.cached()?.pointer("/capabilities/diagnosticProvider")?;
        let inter = provider.get("interFileDependencies").and_then(Value::as_bool);
        Some(inter.unwrap_or(false))
    }

    fn request_pull(&mut self, io: &mut Io, uri: &str) {
        let Some(doc) = self.docs.get(uri) else { return };
        if doc.openers.iter().all(|s| self.client_pulls.contains(s)) {
            return; // every opener pulls for itself
        }
        let bid = io.new_id();
        self.pulls.insert(bid, (uri.to_owned(), doc.version));
        io.to_backend(&json!({
            "jsonrpc": "2.0", "id": bid, "method": "textDocument/diagnostic",
            "params": {"textDocument": {"uri": uri}},
        }));
    }

    /// After a document changed: pull it, and with inter-file dependencies
    /// every other open document too, since their errors may have moved.
    fn after_change(&mut self, io: &mut Io, uri: &str) {
        let Some(inter_file) = self.backend_pull() else { return };
        self.request_pull(io, uri);
        if inter_file {
            let others: Vec<String> =
                self.docs.keys().filter(|u| u.as_str() != uri).cloned().collect();
            for other in others {
                self.request_pull(io, &other);
            }
        }
    }

    fn pull_reply(&mut self, io: &mut Io, uri: String, version: i64, msg: Value) {
        if let Some(err) = msg.get("error") {
            log(&format!("diagnostic pull for {} failed: {}", uri, err));
            return;
        }
        let current = self.docs.get(&uri).map(|d| d.version);
        if current != Some(version) {
            return; // stale: a newer pull for this document is in flight
        }
        let report = &msg["result"];
        let mut reports = vec![(uri, report.clone())];
        if let Some(related) = report.get("relatedDocuments").and_then(Value::as_object) {
            reports.extend(related.iter().map(|(u, r)| (u.clone(), r.clone())));
        }
        for (uri, report) in reports {
            if report.get("kind").and_then(Value::as_str) != Some("full") {
                continue; // "unchanged": what clients have is still right
            }
            let items = report.get("items").cloned().unwrap_or_else(|| json!([]));
            self.diagnostics(io, json!({
                "jsonrpc": "2.0",
                "method": "textDocument/publishDiagnostics",
                "params": {"uri": uri, "diagnostics": items},
            }));
        }
    }

    // -- documents ---------------------------------------------------------

    fn did_open(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let Some(uri) = uri_of(&msg) else { return };
        match self.docs.get_mut(&uri) {
            None => {
                let mut out = msg.clone();
                out["params"]["textDocument"]["version"] = 1.into();
                io.to_backend(&out);
                self.docs.insert(uri.clone(), Doc { openers: BTreeSet::from([sid]), version: 1 });
                self.after_change(io, &uri);
            }
            Some(doc) => {
                // Already open on the server: bring it to this client's text.
                doc.version += 1;
                doc.openers.insert(sid);
                let text = msg.pointer("/params/textDocument/text").cloned().unwrap_or_default();
                io.to_backend(&json!({
                    "jsonrpc": "2.0",
                    "method": "textDocument/didChange",
                    "params": {
                        "textDocument": {"uri": uri, "version": doc.version},
                        "contentChanges": [{"text": text}],
                    },
                }));
                if let Some(diags) = self.diagnostics.get(&uri) {
                    io.send(sid, &json!({
                        "jsonrpc": "2.0",
                        "method": "textDocument/publishDiagnostics",
                        "params": diags,
                    }));
                }
                self.after_change(io, &uri);
            }
        }
    }

    fn did_change(&mut self, io: &mut Io, msg: Value) {
        let Some(uri) = uri_of(&msg) else { return };
        let Some(doc) = self.docs.get_mut(&uri) else {
            log(&format!("didChange for unopened {}, dropped", uri));
            return;
        };
        doc.version += 1;
        let mut out = msg.clone();
        out["params"]["textDocument"]["version"] = doc.version.into();
        io.to_backend(&out);
        self.after_change(io, &uri);
    }

    fn release(&mut self, io: &mut Io, sid: Sid, uri: &str) {
        let Some(doc) = self.docs.get_mut(uri) else { return };
        if !doc.openers.remove(&sid) || !doc.openers.is_empty() {
            return;
        }
        self.docs.remove(uri);
        self.diagnostics.remove(uri);
        io.to_backend(&json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didClose",
            "params": {"textDocument": {"uri": uri}},
        }));
    }

    // -- requests ----------------------------------------------------------

    fn forward_request(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let bid = io.new_id();
        self.calls.insert(bid, (sid, msg["id"].clone()));
        let mut out = with_id(&msg, bid);
        for (field, prefix) in [("workDoneToken", "wd"), ("partialResultToken", "pr")] {
            if let Some(token) = msg.get("params").and_then(|p| p.get(field)) {
                let ours = format!("mux-{}-{}", prefix, bid);
                self.tokens.insert(key(&Value::from(ours.clone())), (sid, token.clone()));
                out["params"][field] = ours.into();
            }
        }
        io.to_backend(&out);
    }

    fn cancel(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        let Some(original) = msg.pointer("/params/id") else { return };
        let bid = self
            .calls
            .iter()
            .find(|(_, (owner, oid))| *owner == sid && oid == original)
            .map(|(bid, _)| *bid);
        // The server still answers a cancelled request, so keep the mapping.
        if let Some(bid) = bid {
            io.to_backend(&json!({
                "jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": bid},
            }));
        }
    }

    fn drop_tokens_of(&mut self, bid: i64) {
        for prefix in ["wd", "pr"] {
            self.tokens.remove(&key(&Value::from(format!("mux-{}-{}", prefix, bid))));
        }
    }

    // -- server -> clients -------------------------------------------------

    fn server_request(&mut self, io: &mut Io, msg: Value) {
        let m = method(&msg).unwrap_or_default().to_owned();
        // Dynamic registration is per-client state the daemon cannot fan out;
        // Claude Code declares no support for it anyway. Accept and keep it.
        if m == "client/registerCapability" || m == "client/unregisterCapability" {
            io.to_backend(&result(&msg["id"], Value::Null));
            return;
        }
        if m == "window/workDoneProgress/create" {
            if let Some(token) = msg.pointer("/params/token") {
                self.own_tokens.insert(key(token));
            }
            io.to_backend(&result(&msg["id"], Value::Null));
            return;
        }
        if m == "workspace/diagnostic/refresh" && self.backend_pull().is_some() {
            io.to_backend(&result(&msg["id"], Value::Null));
            let open: Vec<String> = self.docs.keys().cloned().collect();
            for uri in open {
                self.request_pull(io, &uri);
            }
            return;
        }
        let Some(sid) = io.first_session() else {
            io.to_backend(&error(&msg["id"], INTERNAL_ERROR, "mcpmux: no client attached"));
            return;
        };
        let cid = format!("mux-srv-{}", io.new_id());
        self.server_calls.insert(key(&Value::from(cid.clone())), (sid, msg["id"].clone()));
        io.send(sid, &with_id(&msg, cid));
    }

    fn progress(&mut self, io: &mut Io, msg: Value) {
        let Some(token) = msg.pointer("/params/token") else { return };
        let token_key = key(token);
        if let Some((sid, original)) = self.tokens.get(&token_key) {
            let mut out = msg.clone();
            out["params"]["token"] = original.clone();
            io.send(*sid, &out);
        } else if self.own_tokens.contains(&token_key)
            && msg.pointer("/params/value/kind").and_then(Value::as_str) == Some("end")
        {
            self.own_tokens.remove(&token_key);
        }
    }

    fn diagnostics(&mut self, io: &mut Io, msg: Value) {
        let Some(uri) = msg.pointer("/params/uri").and_then(Value::as_str).map(str::to_owned)
        else {
            return;
        };
        if let Some(doc) = self.docs.get(&uri) {
            for sid in &doc.openers {
                io.send(*sid, &msg);
            }
            self.diagnostics.insert(uri, msg["params"].clone());
        }
    }
}

impl Protocol for Lsp {
    fn client(&mut self, io: &mut Io, sid: Sid, msg: Value) {
        match method(&msg) {
            Some("initialize") => {
                if msg.pointer("/params/capabilities/textDocument/diagnostic").is_some() {
                    self.client_pulls.insert(sid);
                }
                self.init.request(io, sid, msg)
            }
            Some("initialized") => {
                if !self.initialized_sent {
                    self.initialized_sent = true;
                    io.to_backend(&msg);
                }
            }
            // The server outlives clients: answer shutdown ourselves, and
            // treat exit as this client leaving.
            Some("shutdown") => io.send(sid, &result(&msg["id"], Value::Null)),
            Some("exit") => io.close(sid),
            Some("$/cancelRequest") => self.cancel(io, sid, msg),
            Some("textDocument/didOpen") => self.did_open(io, sid, msg),
            Some("textDocument/didChange") => self.did_change(io, msg),
            Some("textDocument/didClose") => {
                if let Some(uri) = uri_of(&msg) {
                    self.release(io, sid, &uri);
                }
            }
            Some(_) if has_id(&msg) => self.forward_request(io, sid, msg),
            Some(_) => io.to_backend(&msg),
            None if has_id(&msg) => {
                if let Some((_, server_id)) = self.server_calls.remove(&key(&msg["id"])) {
                    io.to_backend(&with_id(&msg, server_id));
                }
            }
            None => {}
        }
    }

    fn detach(&mut self, io: &mut Io, sid: Sid) {
        self.init.detach(sid);
        self.client_pulls.remove(&sid);

        let owned: Vec<i64> =
            self.calls.iter().filter(|(_, (o, _))| *o == sid).map(|(b, _)| *b).collect();
        for bid in owned {
            self.calls.remove(&bid);
            self.drop_tokens_of(bid);
            io.to_backend(&json!({
                "jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": bid},
            }));
        }

        let pending: Vec<String> = self
            .server_calls
            .iter()
            .filter(|(_, (o, _))| *o == sid)
            .map(|(k, _)| k.clone())
            .collect();
        for cid in pending {
            if let Some((_, server_id)) = self.server_calls.remove(&cid) {
                io.to_backend(&error(&server_id, REQUEST_CANCELLED, "mcpmux: client disconnected"));
            }
        }

        let opened: Vec<String> = self
            .docs
            .iter()
            .filter(|(_, d)| d.openers.contains(&sid))
            .map(|(u, _)| u.clone())
            .collect();
        for uri in opened {
            self.release(io, sid, &uri);
        }
    }

    fn backend(&mut self, io: &mut Io, msg: Value) {
        match method(&msg) {
            Some("textDocument/publishDiagnostics") => self.diagnostics(io, msg),
            Some("$/progress") => self.progress(io, msg),
            Some(_) if has_id(&msg) => self.server_request(io, msg),
            Some(_) => io.broadcast(&msg),
            None if self.init.is_reply(&msg) => {
                self.init.reply(io, msg);
                // Documents opened before the handshake finished were never
                // pulled: the bridge did not know yet that it was needed.
                if self.backend_pull().is_some() {
                    let open: Vec<String> = self.docs.keys().cloned().collect();
                    for uri in open {
                        self.request_pull(io, &uri);
                    }
                }
            }
            None => {
                let Some(bid) = msg.get("id").and_then(Value::as_i64) else { return };
                if let Some((uri, version)) = self.pulls.remove(&bid) {
                    self.pull_reply(io, uri, version, msg);
                    return;
                }
                let Some((sid, original)) = self.calls.remove(&bid) else { return };
                self.drop_tokens_of(bid);
                io.send(sid, &with_id(&msg, original));
            }
        }
    }

    fn tick(&mut self, io: &mut Io) {
        self.init.tick(io);
    }
}
