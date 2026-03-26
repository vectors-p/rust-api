use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Query, State},
    http::{StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use chrono::Local;
use serde::Deserialize;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

//Constants 

const MAX_CONNECTIONS: usize = 100;
const MAX_MESSAGE_LEN: usize = 500;
const MAX_USERNAME_LEN: usize = 20;
const RATE_LIMIT_MS: u64 = 500; // min ms between messages

//Shared State 

struct AppState {
    tx: broadcast::Sender<String>,
    connection_count: AtomicUsize,
}

//Query Params 

#[derive(Deserialize)]
struct ConnectParams {
    username: Option<String>,
}

//Main

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, _rx) = broadcast::channel(100);
    let state = Arc::new(AppState {
        tx,
        connection_count: AtomicUsize::new(0),
    });

    // Allow only your frontend origin in production
    // replace Any with your actual frontend URL
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(|| async { "Hello from Axum!" }))
        .route("/ws", get(ws_handler))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("────────────────────────────────────");
    println!("  Server running on port 3000");
    println!("  WebSocket: ws://localhost:3000/ws?username=Alice");
    println!("────────────────────────────────────");
    axum::serve(listener, app).await.unwrap();
}

//Helpers

fn timestamp() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

fn sanitize_username(name: &str) -> Option<String> {
    let trimmed = name.trim();

    if trimmed.is_empty() || trimmed.len() > MAX_USERNAME_LEN {
        return None;
    }

    // Only allow letters, numbers and underscores
    if !trimmed.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }

    Some(trimmed.to_string())
}

//WebSocket Handler

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<ConnectParams>,
    State(state): State<Arc<AppState>>,
) -> Response {
    // Reject if server is full
    if state.connection_count.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server is full, try again later")
            .into_response();
    }

    // Sanitize username
    let username = params
        .username
        .and_then(|u| sanitize_username(&u))
        .unwrap_or_else(|| "Anonymous".to_string());

    ws.on_upgrade(move |socket| handle_socket(socket, state, username))
}

//Socket Loop

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, username: String) {
    // Track connection count
    let count = state.connection_count.fetch_add(1, Ordering::Relaxed) + 1;

    let mut rx = state.tx.subscribe();
    let mut last_msg_time = Instant::now();
    let rate_limit = Duration::from_millis(RATE_LIMIT_MS);

    // Announce join
    let join_msg = format!("[{}]  {} joined ({} online)", timestamp(), username, count);
    println!("{join_msg}");
    let _ = state.tx.send(join_msg);

    loop {
        tokio::select! {
            // Incoming message from this client
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Rate limit
                        if last_msg_time.elapsed() < rate_limit {
                            let warn = "⚠️ You're sending too fast, slow down!".to_string();
                            let _ = socket.send(Message::Text(warn.into())).await;
                            continue;
                        }
                        last_msg_time = Instant::now();

                        // Reject empty messages
                        let text = text.trim().to_string();
                        if text.is_empty() {
                            continue;
                        }

                        // Reject messages that are too long
                        if text.len() > MAX_MESSAGE_LEN {
                            let warn = format!("⚠️ Message too long (max {} chars)", MAX_MESSAGE_LEN);
                            let _ = socket.send(Message::Text(warn.into())).await;
                            continue;
                        }

                        let formatted = format!("[{}] {}: {}", timestamp(), username, text);
                        println!("{formatted}");
                        let _ = state.tx.send(formatted);
                    }

                    // Client disconnected
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }

                    _ => {}
                }
            }

            // ── Broadcast message to this client ──
            Ok(msg) = rx.recv() => {
                if socket.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }

    // Decrement connection count
    let count = state.connection_count.fetch_sub(1, Ordering::Relaxed) - 1;

    // Announce leave
    let leave_msg = format!("[{}]  {} left ({} online)", timestamp(), username, count);
    println!("{leave_msg}");
    let _ = state.tx.send(leave_msg);
}