use std::str::FromStr;
use std::sync::Arc;

use axum::{routing::get, Json, Router};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};
use socketioxide::{
    extract::{Data, SocketRef, State},
    SocketIo,
};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

type OnlineMap = DashMap<String, String>;
type SidMap = DashMap<String, String>;
/// username -> the other username in the current call (both directions).
type PeerMap = DashMap<String, String>;

#[derive(Clone, Default)]
struct AppState {
    online: Arc<OnlineMap>,
    sid_user: Arc<SidMap>,
    peer: Arc<PeerMap>,
}

#[inline]
fn norm(s: &str) -> String {
    s.trim().to_lowercase()
}

fn registered_user(st: &AppState, sid: &str) -> Option<String> {
    st.sid_user.get(sid).map(|u| u.clone()).filter(|s| !s.is_empty())
}

fn is_online(st: &AppState, user: &str) -> bool {
    st.online.contains_key(user)
}

fn linked(st: &AppState, a: &str, b: &str) -> bool {
    if a.is_empty() || b.is_empty() || a == b {
        return false;
    }
    st.peer.get(a).map(|p| p.as_str() == b).unwrap_or(false)
}

fn link_pair(st: &AppState, a: &str, b: &str) {
    st.peer.insert(a.to_string(), b.to_string());
    st.peer.insert(b.to_string(), a.to_string());
}

/// Drop `user` from the call map and return the partner that was linked.
fn unlink_user(st: &AppState, user: &str) -> Option<String> {
    let other = st.peer.remove(user).map(|(_, v)| v)?;
    st.peer.remove_if(&other, |_, v| v == user);
    Some(other)
}

fn emit_to_user(socket: &SocketRef, user: &str, event: &'static str, data: Value) {
    if let Err(e) = socket.to(user.to_string()).emit(event, &data) {
        warn!("emit {} -> {} failed: {:?}", event, user, e);
    }
}

fn emit_call_ended(socket: &SocketRef, to: &str, from: &str, reason: &str) {
    emit_to_user(
        socket,
        to,
        "call_ended",
        json!({ "from": from, "reason": reason }),
    );
}

/// End `user`'s current call and tell the partner.
fn hangup_existing(socket: &SocketRef, st: &AppState, user: &str, reason: &str) -> Option<String> {
    let other = unlink_user(st, user)?;
    emit_call_ended(socket, &other, user, reason);
    Some(other)
}

fn require_registered(socket: &SocketRef, st: &AppState) -> Option<String> {
    let from = registered_user(st, &socket.id.to_string());
    if from.is_none() {
        let _ = socket.emit("call_error", json!({ "message": "not_registered" }));
    }
    from
}

#[derive(Debug, Deserialize)]
struct RegisterPayload {
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallUserPayload {
    to: Option<String>,
    offer: Option<Value>,
    #[serde(rename = "callType")]
    call_type: Option<String>,
    #[serde(default, rename = "iceRestart")]
    ice_restart: bool,
}

#[derive(Debug, Deserialize)]
struct AnswerPayload {
    to: Option<String>,
    answer: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct IcePayload {
    to: Option<String>,
    candidate: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct EndPayload {
    to: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MediaPayload {
    to: Option<String>,
    video: Option<Value>,
    audio: Option<Value>,
}

fn kick_previous_socket(io: &SocketIo, username: &str, old_sid: &str) {
    let Ok(sid) = socketioxide::socket::Sid::from_str(old_sid) else {
        warn!("[register] bad previous sid={old_sid}");
        return;
    };
    let Some(old) = io.get_socket(sid) else {
        info!("[register] previous socket already gone user={username} sid={old_sid}");
        return;
    };
    let _ = old.emit(
        "session_replaced",
        json!({ "username": username }),
    );
    let _ = old.leave(username.to_string());
    if let Err(e) = old.disconnect() {
        warn!("[register] disconnect previous user={username} err={e:?}");
    } else {
        info!("[register] replaced previous session user={username} sid={old_sid}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let state = AppState::default();
    let (layer, io) = SocketIo::builder().with_state(state.clone()).build_layer();

    io.ns("/", |socket: SocketRef, state: State<AppState>| {
        let sid = socket.id.to_string();
        info!("[connect] sid={sid}");
        {
            let st = state.clone();
            let sock = socket.clone();
            let sid = sid.clone();
            socket.on_disconnect(move || {
                let st = st.clone();
                let sock = sock.clone();
                let sid = sid.clone();
                async move {
                    let Some((_, username)) = st.sid_user.remove(&sid) else {
                        info!("[disconnect] sid={sid} (unregistered)");
                        return;
                    };
                    // Only the socket that currently owns the username may clear presence.
                    // remove_if is atomic, so a newer register cannot be deleted here.
                    let removed = st.online.remove_if(&username, |_, current| current == &sid);
                    if removed.is_none() {
                        info!("[disconnect] stale sid={sid} user={username}");
                        return;
                    }
                    let _ = sock.leave(username.clone());
                    if st.online.contains_key(&username) {
                        info!("[disconnect] user={username} already re-registered");
                        return;
                    }
                    let payload = json!({ "username": username });
                    let _ = sock.broadcast().emit("user_offline", &payload);
                    // Older clients listened for this name. Same payload, same meaning.
                    let _ = sock.broadcast().emit("user_disconnected", &payload);
                    info!("[disconnect] user={username} sid={sid}");
                }
            });
        }

        socket.on(
            "register",
            |socket: SocketRef, io: SocketIo, Data(data): Data<RegisterPayload>, state: State<AppState>| async move {
                let Some(username) = data
                    .username
                    .as_deref()
                    .map(norm)
                    .filter(|s| !s.is_empty())
                else {
                    let _ = socket.emit(
                        "register_error",
                        json!({ "message": "username required" }),
                    );
                    return;
                };

                let sid = socket.id.to_string();

                // Same socket re-registering (reconnect handler fired twice) is a no-op takeover.
                if let Some(old_sid) = state.online.insert(username.clone(), sid.clone()) {
                    if old_sid != sid {
                        state.sid_user.remove(&old_sid);
                        // This device does not keep the previous peer connection.
                        hangup_existing(&socket, &state, &username, "replaced");
                        kick_previous_socket(&io, &username, &old_sid);
                    }
                }

                if let Err(e) = socket.join(username.clone()) {
                    warn!("[register] join({username}) err={e:?}");
                }

                state.sid_user.insert(sid, username.clone());

                let online: Vec<String> = state.online.iter().map(|e| e.key().clone()).collect();
                let _ = socket.emit(
                    "registered",
                    json!({ "username": username, "online": online }),
                );
                let _ = socket
                    .broadcast()
                    .emit("user_online", json!({ "username": username }));
                info!("[register] {username} sid={}", socket.id);
            },
        );

        socket.on(
            "call_user",
            |socket: SocketRef, Data(data): Data<CallUserPayload>, state: State<AppState>| async move {
                let Some(from) = require_registered(&socket, &state) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();

                if to.is_empty() || data.offer.is_none() {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "missing to/offer" }),
                    );
                    return;
                }
                if to == from {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "cannot_call_self" }),
                    );
                    return;
                }
                if !is_online(&state, &to) {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "user_offline", "to": to }),
                    );
                    return;
                }

                let restart = data.ice_restart && linked(&state, &from, &to);
                if !restart {
                    if state
                        .peer
                        .get(&from)
                        .map(|p| p.as_str() != to)
                        .unwrap_or(false)
                    {
                        hangup_existing(&socket, &state, &from, "replaced");
                    }
                    if state
                        .peer
                        .get(&to)
                        .map(|p| p.as_str() != from)
                        .unwrap_or(false)
                    {
                        hangup_existing(&socket, &state, &to, "replaced");
                    }
                    link_pair(&state, &from, &to);
                }

                emit_to_user(
                    &socket,
                    &to,
                    "incoming_call",
                    json!({
                        "from": from,
                        "offer": data.offer,
                        "callType": data.call_type.unwrap_or_else(|| "video".into()),
                        "iceRestart": data.ice_restart,
                    }),
                );
                info!(
                    "[call_user] {from} -> {to} iceRestart={}",
                    data.ice_restart
                );
            },
        );

        socket.on(
            "make_answer",
            |socket: SocketRef, Data(data): Data<AnswerPayload>, state: State<AppState>| async move {
                let Some(from) = require_registered(&socket, &state) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || data.answer.is_none() {
                    return;
                }
                if !linked(&state, &from, &to) {
                    warn!("[make_answer] {from} -> {to} ignored (not in a call)");
                    return;
                }
                if !is_online(&state, &to) {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "user_offline", "to": to }),
                    );
                    return;
                }
                emit_to_user(
                    &socket,
                    &to,
                    "call_answered",
                    json!({ "from": from, "answer": data.answer }),
                );
                info!("[make_answer] {from} -> {to}");
            },
        );

        socket.on(
            "ice_candidate",
            |socket: SocketRef, Data(data): Data<IcePayload>, state: State<AppState>| async move {
                let Some(from) = registered_user(&state, &socket.id.to_string()) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || data.candidate.is_none() {
                    return;
                }
                if !linked(&state, &from, &to) {
                    return;
                }
                emit_to_user(
                    &socket,
                    &to,
                    "ice_candidate",
                    json!({ "from": from, "candidate": data.candidate }),
                );
            },
        );

        socket.on(
            "end_call",
            |socket: SocketRef, Data(data): Data<EndPayload>, state: State<AppState>| async move {
                let Some(from) = registered_user(&state, &socket.id.to_string()) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || !linked(&state, &from, &to) {
                    return;
                }
                unlink_user(&state, &from);
                let reason = data.reason.unwrap_or_else(|| "hangup".into());
                emit_call_ended(&socket, &to, &from, &reason);
                info!("[end_call] {from} -> {to} ({reason})");
            },
        );

        socket.on(
            "reject_call",
            |socket: SocketRef, Data(data): Data<EndPayload>, state: State<AppState>| async move {
                let Some(from) = registered_user(&state, &socket.id.to_string()) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || !linked(&state, &from, &to) {
                    return;
                }
                unlink_user(&state, &from);
                emit_call_ended(&socket, &to, &from, "reject");
                info!("[reject_call] {from} -> {to}");
            },
        );

        socket.on(
            "media_state",
            |socket: SocketRef, Data(data): Data<MediaPayload>, state: State<AppState>| async move {
                let Some(from) = registered_user(&state, &socket.id.to_string()) else {
                    return;
                };
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || !linked(&state, &from, &to) {
                    return;
                }
                emit_to_user(
                    &socket,
                    &to,
                    "media_state",
                    json!({
                        "from": from,
                        "video": data.video,
                        "audio": data.audio,
                    }),
                );
            },
        );
    });

    let health = state.clone();
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let st = health.clone();
                async move {
                    Json(json!({
                        "ok": true,
                        "service": "Z-Chat WebRTC Signaling",
                        "online": st.online.len(),
                    }))
                }
            }),
        )
        .route(
            "/health",
            get(move || async { Json(json!({ "status": "ok" })) }),
        )
        .layer(layer)
        .layer(CorsLayer::permissive());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5000);
    let addr = format!("0.0.0.0:{port}");
    info!("Z-Chat signaling listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
