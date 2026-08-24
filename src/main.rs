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

#[derive(Clone, Default)]
struct AppState {
    online: Arc<OnlineMap>,
    sid_user: Arc<SidMap>,
}

#[inline]
fn norm(s: &str) -> String {
    s.trim().to_lowercase()
}

fn from_user(payload: &Option<String>, sid: &str, st: &AppState) -> String {
    if let Some(f) = payload {
        let n = norm(f);
        if !n.is_empty() {
            return n;
        }
    }
    st.sid_user.get(sid).map(|u| u.clone()).unwrap_or_default()
}

fn is_online(st: &AppState, user: &str) -> bool {
    st.online.contains_key(user)
}

fn emit_to_user(socket: &SocketRef, user: &str, event: &'static str, data: Value) {
    if let Err(e) = socket.to(user.to_string()).emit(event, data) {
        warn!("emit {} -> {} failed: {:?}", event, user, e);
    }
}

#[derive(Debug, Deserialize)]
struct RegisterPayload {
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallUserPayload {
    to: Option<String>,
    from: Option<String>,
    offer: Option<Value>,
    #[serde(rename = "callType")]
    call_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnswerPayload {
    to: Option<String>,
    from: Option<String>,
    answer: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct IcePayload {
    to: Option<String>,
    from: Option<String>,
    candidate: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct EndPayload {
    to: Option<String>,
    from: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MediaPayload {
    to: Option<String>,
    from: Option<String>,
    video: Option<Value>,
    audio: Option<Value>,
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
                    let still_me = st
                        .online
                        .get(&username)
                        .map(|s| s.as_str() == sid)
                        .unwrap_or(false);
                    if still_me {
                        st.online.remove(&username);
                        let _ = sock.leave(username.clone());
                        let _ = sock
                            .broadcast()
                            .emit("user_offline", json!({ "username": username }));
                        info!("[disconnect] user={username} sid={sid}");
                    } else {
                        info!("[disconnect] stale sid={sid} user={username}");
                    }
                }
            });
        }
        
        socket.on(
            "register",
            |socket: SocketRef, Data(data): Data<RegisterPayload>, state: State<AppState>| async move {
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

                if let Some((_, old_sid)) = state.online.remove(&username) {
                    if old_sid != sid {
                        state.sid_user.remove(&old_sid);
                    }
                }

                if let Err(e) = socket.join(username.clone()) {
                    warn!("[register] join({username}) err={e:?}");
                }

                state.online.insert(username.clone(), sid.clone());
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
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                let from = from_user(&data.from, &socket.id.to_string(), &state);

                if to.is_empty() || from.is_empty() || data.offer.is_none() {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "missing to/from/offer" }),
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

                emit_to_user(
                    &socket,
                    &to,
                    "incoming_call",
                    json!({
                        "from": from,
                        "offer": data.offer,
                        "callType": data.call_type.unwrap_or_else(|| "video".into()),
                    }),
                );
                info!("[call_user] {from} -> {to}");
            },
        );

        socket.on(
            "make_answer",
            |socket: SocketRef, Data(data): Data<AnswerPayload>, state: State<AppState>| async move {
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                let from = from_user(&data.from, &socket.id.to_string(), &state);
                if to.is_empty() || data.answer.is_none() {
                    return;
                }
                if !is_online(&state, &to) {
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
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() || data.candidate.is_none() {
                    return;
                }
                let from = from_user(&data.from, &socket.id.to_string(), &state);
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
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() {
                    return;
                }
                let from = from_user(&data.from, &socket.id.to_string(), &state);
                let reason = data.reason.unwrap_or_else(|| "hangup".into());
                emit_to_user(
                    &socket,
                    &to,
                    "call_ended",
                    json!({ "from": from, "reason": reason }),
                );
                info!("[end_call] {from} -> {to} ({reason})");
            },
        );

        socket.on(
            "reject_call",
            |socket: SocketRef, Data(data): Data<EndPayload>, state: State<AppState>| async move {
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() {
                    return;
                }
                let from = from_user(&data.from, &socket.id.to_string(), &state);
                emit_to_user(
                    &socket,
                    &to,
                    "call_ended",
                    json!({ "from": from, "reason": "reject" }),
                );
                info!("[reject_call] {from} -> {to}");
            },
        );

        socket.on(
            "media_state",
            |socket: SocketRef, Data(data): Data<MediaPayload>, state: State<AppState>| async move {
                let to = data.to.as_deref().map(norm).unwrap_or_default();
                if to.is_empty() {
                    return;
                }
                let from = from_user(&data.from, &socket.id.to_string(), &state);
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
