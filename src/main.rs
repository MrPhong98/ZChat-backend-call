use std::sync::Arc;
use axum::{routing::get, Json, Router};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::json;
use socketioxide::{
    extract::{Data, SocketRef, State},
    SocketIo,
};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

type OnlineUsers = DashMap<String, String>;
type SidToUser = DashMap<String, String>;

#[derive(Clone, Default)]
struct AppState {
    online_users: Arc<OnlineUsers>,
    sid_to_user: Arc<SidToUser>,
}

fn norm(name: &str) -> String {
    name.trim().to_lowercase()
}

fn resolve_from(payload_from: &Option<String>, sid: &str, state: &AppState) -> String {
    if let Some(f) = payload_from {
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

#[derive(Debug, Deserialize)]
struct RegisterPayload {
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallUserPayload {
    to: Option<String>,
    from: Option<String>,
    offer: Option<serde_json::Value>,
    #[serde(rename = "callType")]
    call_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MakeAnswerPayload {
    to: Option<String>,
    from: Option<String>,
    answer: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct IceCandidatePayload {
    to: Option<String>,
    from: Option<String>,
    candidate: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct EndCallPayload {
    to: Option<String>,
    from: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MediaStatePayload {
    to: Option<String>,
    from: Option<String>,
    video: Option<serde_json::Value>,
    audio: Option<serde_json::Value>,
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
        info!("[connect] sid={}", socket.id);
        let sid_str = socket.id.to_string();

        {
            let st = state.clone();
            let sref = socket.clone();
            let sid_str = sid_str.clone();
            socket.on_disconnect(move || {
                let st = st.clone();
                let sref = sref.clone();
                let sid_str = sid_str.clone();
                async move {
                    if let Some((_, username)) = st.sid_to_user.remove(&sid_str) {
                        // Chỉ dọn dẹp nếu session ngắt kết nối đúng là session hiện tại
                        let is_current = st
                            .online_users
                            .get(&username)
                            .map(|s| s.value() == &sid_str)
                            .unwrap_or(false);

                        if is_current {
                            st.online_users.remove(&username);
                            let _ = sref.broadcast().emit(
                                "user_offline",
                                json!({ "username": username }),
                            );
                        }
                    }
                    info!("[disconnect] sid={}", sid_str);
                }
            });
        }

        socket.on(
            "register",
            |socket: SocketRef, Data(data): Data<RegisterPayload>, state: State<AppState>| async move {
                let username = data
                    .username
                    .as_deref()
                    .map(norm)
                    .filter(|s| !s.is_empty());

                let Some(username) = username else {
                    let _ = socket.emit(
                        "register_error",
                        json!({ "message": "username required" }),
                    );
                    return;
                };

                let sid = socket.id.to_string();

                // Dọn dẹp session cũ hoàn toàn
                if let Some((_, old_sid)) = state.online_users.remove(&username) {
                    if old_sid != sid {
                        state.sid_to_user.remove(&old_sid);
                    }
                }

                // Gia nhập Room theo username
                if let Err(e) = socket.join(username.clone()) {
                    warn!("[register] join room failed for {}: {:?}", username, e);
                }

                state.online_users.insert(username.clone(), sid.clone());
                state.sid_to_user.insert(sid.clone(), username.clone());

                let online: Vec<String> = state
                    .online_users
                    .iter()
                    .map(|e| e.key().clone())
                    .collect();

                let _ = socket.emit(
                    "registered",
                    json!({ "username": &username, "online": online }),
                );
                let _ = socket
                    .broadcast()
                    .emit("user_online", json!({ "username": &username }));
                
                info!("[register] success user={} sid={}", username, socket.id);
            },
        );

        socket.on(
            "call_user",
            |socket: SocketRef, Data(data): Data<CallUserPayload>, state: State<AppState>| async move {
                let to_user = data.to.as_deref().map(norm).unwrap_or_default();
                let from_user = resolve_from(&data.from, &socket.id.to_string(), &state);

                if to_user.is_empty() || from_user.is_empty() || data.offer.is_none() {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "missing to/from/offer" }),
                    );
                    return;
                }

                if !state.online_users.contains_key(&to_user) {
                    let _ = socket.emit(
                        "call_error",
                        json!({ "message": "user_offline", "to": to_user }),
                    );
                    return;
                }

                let payload = json!({
                    "from": from_user,
                    "offer": data.offer,
                    "callType": data.call_type.unwrap_or_else(|| "video".into()),
                });

                let _ = socket.to(to_user.clone()).emit("incoming_call", payload);
                info!("[call_user] {} -> {}", from_user, to_user);
            },
        );

        socket.on(
            "make_answer",
            |socket: SocketRef, Data(data): Data<MakeAnswerPayload>, state: State<AppState>| async move {
                let to_user = data.to.as_deref().map(norm).unwrap_or_default();
                let from_user = resolve_from(&data.from, &socket.id.to_string(), &state);

                if to_user.is_empty() || data.answer.is_none() {
                    return;
                }

                let _ = socket.to(to_user.clone()).emit(
                    "call_answered",
                    json!({ "from": from_user, "answer": data.answer }),
                );
                info!("[make_answer] {} -> {}", from_user, to_user);
            },
        );

        socket.on(
            "ice_candidate",
            |socket: SocketRef, Data(data): Data<IceCandidatePayload>, state: State<AppState>| async move {
                let to_user = data.to.as_deref().map(norm).unwrap_or_default();
                if to_user.is_empty() || data.candidate.is_none() {
                    return;
                }
                
                let from_user = resolve_from(&data.from, &socket.id.to_string(), &state);
                
                // Trực tiếp broadcast vào room đối phương
                let _ = socket.to(to_user).emit(
                    "ice_candidate",
                    json!({ "from": from_user, "candidate": data.candidate }),
                );
            },
        );

        socket.on(
            "end_call",
            |socket: SocketRef, Data(data): Data<EndCallPayload>, state: State<AppState>| async move {
                let to_user = data.to.as_deref().map(norm).unwrap_or_default();
                if to_user.is_empty() {
                    return;
                }
                let from_user = resolve_from(&data.from, &socket.id.to_string(), &state);
                let reason = data.reason.unwrap_or_else(|| "hangup".into());

                let _ = socket.to(to_user.clone()).emit(
                    "call_ended",
                    json!({ "from": from_user, "reason": reason }),
                );
                info!("[end_call] {} -> {} ({})", from_user, to_user, reason);
            },
        );

        socket.on(
            "media_state",
            |socket: SocketRef, Data(data): Data<MediaStatePayload>, state: State<AppState>| async move {
                let to_user = data.to.as_deref().map(norm).unwrap_or_default();
                if to_user.is_empty() {
                    return;
                }
                let from_user = resolve_from(&data.from, &socket.id.to_string(), &state);
                
                let _ = socket.to(to_user).emit(
                    "media_state",
                    json!({
                        "from": from_user,
                        "video": data.video,
                        "audio": data.audio,
                    }),
                );
            },
        );
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
    info!("Z-Chat signaling (Rust) running on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
