use std::sync::Arc;
use axum::{routing::get, Json, Router};
use dashmap::DashMap;
use serde_json::{json, Value};
use socketioxide::{
    extract::{Data, SocketRef},
    SocketIo,
};
use tower_http::cors::CorsLayer;
use tracing::info;

type OnlineUsers = DashMap<String, String>; // username -> sid
type SidToUser = DashMap<String, String>;   // sid -> username

#[derive(Clone, Default)]
struct AppState {
    online_users: Arc<OnlineUsers>,
    sid_to_user: Arc<SidToUser>,
}

fn norm(name: &str) -> String {
    name.trim().to_lowercase()
}

fn resolve_from(payload: &Value, sid: &str, state: &AppState) -> String {
    if let Some(f) = payload.get("from").and_then(|v| v.as_str()) {
        let n = norm(f);
        if !n.is_empty() {
            return n;
        }
    }
    state
        .sid_to_user
        .get(sid)
        .map(|u| u.clone())
        .unwrap_or_default()
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

    let io_state = state.clone();
    io.ns("/", move |socket: SocketRef| {
        info!("[connect] sid={}", socket.id);
        let sid_str = socket.id.to_string();

        // disconnect → user_offline
        {
            let st = io_state.clone();
            let sref = socket.clone();
            let sid_str = sid_str.clone();
            socket.on_disconnect(move || {
                let st = st.clone();
                let sref = sref.clone();
                let sid_str = sid_str.clone();
                async move {
                    let username = st.sid_to_user.remove(&sid_str).map(|(_, u)| u);
                    if let Some(ref username) = username {
                        let still_this = st
                            .online_users
                            .get(username)
                            .map(|s| s.as_str() == sid_str)
                            .unwrap_or(false);
                        if still_this {
                            st.online_users.remove(username);
                            let _ = sref
                                .broadcast()
                                .emit("user_offline", json!({ "username": username }));
                        }
                    }
                    info!("[disconnect] sid={} user={:?}", sid_str, username);
                }
            });
        }

        // ── register ────────────────────────────────────────
        let st = io_state.clone();
        socket.on("register", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let username = data.get("username").and_then(|v| v.as_str()).map(norm);
                let Some(username) = username.filter(|s| !s.is_empty()) else {
                    let _ = socket.emit("register_error", json!({ "message": "username required" }));
                    return;
                };

                let sid = socket.id.to_string();

                if let Some((_, old_sid)) = st.online_users.remove(&username) {
                    if old_sid != sid {
                        st.sid_to_user.remove(&old_sid);
                    }
                }

                st.online_users.insert(username.clone(), sid.clone());
                st.sid_to_user.insert(sid, username.clone());

                let online: Vec<String> = st.online_users.iter().map(|e| e.key().clone()).collect();

                let _ = socket.emit("registered", json!({ "username": &username, "online": online }));
                let _ = socket.broadcast().emit("user_online", json!({ "username": &username }));
                info!("[register] {} -> {}", username, socket.id);
            }
        });

        // ── call_user ───────────────────────────────────────
        let st = io_state.clone();
        socket.on("call_user", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                let offer = data.get("offer");

                if to_user.is_empty() || from_user.is_empty() || offer.is_none() {
                    let _ = socket.emit("call_error", json!({ "message": "missing to/from/offer" }));
                    return;
                }

                let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) else {
                    let _ = socket.emit("call_error", json!({ "message": "user_offline", "to": to_user }));
                    return;
                };

                let call_type = data.get("callType").and_then(|v| v.as_str()).unwrap_or("video");

                let _ = socket.to(target).emit(
                    "incoming_call",
                    json!({
                        "from": from_user,
                        "offer": offer,
                        "callType": call_type,
                    }),
                );
                info!("[call_user] {} -> {}", from_user, to_user);
            }
        });

        // ── make_answer ─────────────────────────────────────
        let st = io_state.clone();
        socket.on("make_answer", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                let answer = data.get("answer");

                if to_user.is_empty() || answer.is_none() {
                    let _ = socket.emit("call_error", json!({ "message": "missing to/answer" }));
                    return;
                }

                let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) else {
                    let _ = socket.emit("call_error", json!({ "message": "user_offline", "to": to_user }));
                    return;
                };

                let _ = socket.to(target).emit(
                    "call_answered",
                    json!({ "from": from_user, "answer": answer }),
                );
                info!("[make_answer] {} -> {}", from_user, to_user);
            }
        });

        // ── ice_candidate ───────────────────────────────────
        let st = io_state.clone();
        socket.on("ice_candidate", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                let candidate = data.get("candidate");
                if to_user.is_empty() || candidate.is_none() {
                    return;
                }
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) else {
                    return;
                };
                let _ = socket.to(target).emit(
                    "ice_candidate",
                    json!({ "from": from_user, "candidate": candidate }),
                );
            }
        });

        // ── end_call ────────────────────────────────────────
        let st = io_state.clone();
        socket.on("end_call", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                if to_user.is_empty() {
                    return;
                }
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                let reason = data.get("reason").and_then(|v| v.as_str()).unwrap_or("hangup");
                if let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) {
                    let _ = socket.to(target).emit(
                        "call_ended",
                        json!({ "from": from_user, "reason": reason }),
                    );
                }
                info!("[end_call] {} -> {} ({})", from_user, to_user, reason);
            }
        });

        // ── reject_call ─────────────────────────────────────
        let st = io_state.clone();
        socket.on("reject_call", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                if to_user.is_empty() {
                    return;
                }
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                if let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) {
                    let _ = socket.to(target).emit(
                        "call_ended",
                        json!({ "from": from_user, "reason": "reject" }),
                    );
                }
                info!("[reject_call] {} -> {}", from_user, to_user);
            }
        });

        // ── media_state ─────────────────────────────────────
        let st = io_state.clone();
        socket.on("media_state", move |socket: SocketRef, Data(data): Data<Value>| {
            let st = st.clone();
            async move {
                let to_user = data.get("to").and_then(|v| v.as_str()).map(norm).unwrap_or_default();
                if to_user.is_empty() {
                    return;
                }
                let from_user = resolve_from(&data, &socket.id.to_string(), &st);
                let Some(target) = st.online_users.get(&to_user).map(|s| s.clone()) else {
                    return;
                };
                let _ = socket.to(target).emit(
                    "media_state",
                    json!({
                        "from": from_user,
                        "video": data.get("video"),
                        "audio": data.get("audio"),
                    }),
                );
            }
        });
    });

    let health_state = state.clone();
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let st = health_state.clone();
                async move {
                    Json(json!({
                        "ok": true,
                        "service": "Z-Chat WebRTC Signaling",
                        "online": st.online_users.len(),
                    }))
                }
            }),
        )
        .layer(layer)
        .layer(CorsLayer::permissive());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5000);
    let addr = format!("0.0.0.0:{port}");
    info!("Z-Chat signaling (Rust) listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
