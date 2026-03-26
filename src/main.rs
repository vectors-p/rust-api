use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{State, Query},
    response::Response,
    routing::get,
    Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::broadcast;
use chrono::Local;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
}

#[derive(Deserialize)]
struct ConnectParams {
    username: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, _rx) = broadcast::channel(100);
    let state = Arc::new(AppState { tx });

    let app = Router::new()
        .route("/", get(|| async { "Hello from Axum!" }))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<ConnectParams>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let username = params.username.unwrap_or_else(|| "Anonymous".to_string());
    ws.on_upgrade(move |socket| handle_socket(socket, state, username))
}


fn timestamp() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, username: String) {
    let mut rx = state.tx.subscribe();

    let join_msg = format!("[{}]  {} joined the chat", timestamp(), username);
    println!("{join_msg}");
    let _ = state.tx.send(join_msg);

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let formatted = format!("[{}] {}: {}", timestamp(), username, text);
                        println!("{formatted}");
                        let _ = state.tx.send(formatted);
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        let leave_msg = format!("[{}]  {} left the chat", timestamp(), username);
                        println!("{leave_msg}");
                        let _ = state.tx.send(leave_msg);
                        break;
                    }
                    _ => {}
                }
            }

            Ok(msg) = rx.recv() => {
                if socket.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }
}