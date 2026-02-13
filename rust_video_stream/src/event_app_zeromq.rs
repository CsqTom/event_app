use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::event_app_redis::{EventAppRedis, RedisConfig};

#[cfg(windows)]
#[link(name = "advapi32")]
unsafe extern "system" {}

#[derive(Clone)]
pub struct ZmqConfig {
    pub event_endpoint: String,
    pub rpc_endpoint: String,
}

impl Default for ZmqConfig {
    fn default() -> Self {
        if cfg!(windows) {
            return Self {
                event_endpoint: "tcp://127.0.0.1:5555".to_string(),
                rpc_endpoint: "tcp://127.0.0.1:5556".to_string(),
            };
        }
        let temp_dir = std::env::temp_dir();
        let event_path = temp_dir.join("event_app_event.sock");
        let rpc_path = temp_dir.join("event_app_rpc.sock");
        Self {
            event_endpoint: format!("ipc://{}", event_path.to_string_lossy().replace('\\', "/")),
            rpc_endpoint: format!("ipc://{}", rpc_path.to_string_lossy().replace('\\', "/")),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct EventMessage {
    pub event_type: String,
    pub data: Value,
    pub need_response: bool,
    pub request_id: Option<String>,
    pub timestamp: Option<f64>,
    pub timeout: Option<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct RpcResponse {
    pub request_id: Option<String>,
    pub data: Option<Value>,
    pub error: Option<String>,
}

pub fn serialize_message(message: &EventMessage) -> Result<Vec<u8>, String> {
    serde_json::to_vec(message).map_err(|e| e.to_string())
}

pub fn deserialize_message(payload: &[u8]) -> Result<EventMessage, String> {
    serde_json::from_slice(payload).map_err(|e| e.to_string())
}

pub fn serialize_response(response: &RpcResponse) -> Result<Vec<u8>, String> {
    serde_json::to_vec(response).map_err(|e| e.to_string())
}

pub fn deserialize_response(payload: &[u8]) -> Result<RpcResponse, String> {
    serde_json::from_slice(payload).map_err(|e| e.to_string())
}

type SubHandler = Box<dyn Fn(Value) + Send + Sync>;
type RpcHandler = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

pub struct EventApp {
    event_endpoint: String,
    rpc_endpoint: String,
    subscribers: HashMap<String, Vec<SubHandler>>,
    rpc_handlers: HashMap<String, RpcHandler>,
    context: zmq::Context,
    event_push: Option<zmq::Socket>,
    rpc_req: Option<zmq::Socket>,
    redis_app: Option<EventAppRedis>,
}

impl EventApp {
    pub fn new(config: Option<ZmqConfig>) -> Self {
        let use_redis = cfg!(windows) && config.is_none();
        let config = config.unwrap_or_default();
        let redis_app = if use_redis {
            EventAppRedis::new(Some(RedisConfig::default()), None).ok()
        } else {
            None
        };
        Self {
            event_endpoint: config.event_endpoint,
            rpc_endpoint: config.rpc_endpoint,
            subscribers: HashMap::new(),
            rpc_handlers: HashMap::new(),
            context: zmq::Context::new(),
            event_push: None,
            rpc_req: None,
            redis_app,
        }
    }

    pub fn event_endpoint(&self) -> &str {
        &self.event_endpoint
    }

    pub fn rpc_endpoint(&self) -> &str {
        &self.rpc_endpoint
    }

    pub fn subscribe<F>(&mut self, event_type: &str, handler: F)
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        if let Some(app) = self.redis_app.as_mut() {
            app.subscribe(event_type, handler);
            return;
        }
        let entry = self.subscribers.entry(event_type.to_string()).or_default();
        entry.push(Box::new(handler));
    }

    pub fn rpc<F>(&mut self, event_type: &str, handler: F)
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        if let Some(app) = self.redis_app.as_mut() {
            app.rpc(event_type, handler);
            return;
        }
        self.rpc_handlers
            .insert(event_type.to_string(), Box::new(handler));
    }

    fn ensure_event_push(&mut self) -> Result<&zmq::Socket, String> {
        if self.event_push.is_none() {
            let socket = self.context.socket(zmq::PUSH).map_err(|e| e.to_string())?;
            socket
                .connect(self.event_endpoint())
                .map_err(|e| e.to_string())?;
            self.event_push = Some(socket);
        }
        Ok(self.event_push.as_ref().unwrap())
    }

    fn ensure_rpc_req(&mut self) -> Result<&zmq::Socket, String> {
        if self.rpc_req.is_none() {
            let socket = self.context.socket(zmq::REQ).map_err(|e| e.to_string())?;
            socket
                .connect(self.rpc_endpoint())
                .map_err(|e| e.to_string())?;
            self.rpc_req = Some(socket);
        }
        Ok(self.rpc_req.as_ref().unwrap())
    }

    pub fn publish(&mut self, event_type: &str, data: Value) -> Result<(), String> {
        if let Some(app) = self.redis_app.as_mut() {
            return app.publish(event_type, data);
        }
        let message = EventMessage {
            event_type: event_type.to_string(),
            data,
            need_response: false,
            request_id: None,
            timestamp: None,
            timeout: None,
        };
        let payload = serialize_message(&message)?;
        let socket = self.ensure_event_push()?;
        socket.send(payload, 0).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn get(&mut self, event_type: &str, data: Value, timeout: f64) -> Result<Value, String> {
        if let Some(app) = self.redis_app.as_mut() {
            return app.get(event_type, data, timeout);
        }
        let request_id = Uuid::new_v4().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs_f64();
        let message = EventMessage {
            event_type: event_type.to_string(),
            data,
            need_response: true,
            request_id: Some(request_id.clone()),
            timestamp: Some(timestamp),
            timeout: Some(timeout),
        };
        let payload = serialize_message(&message)?;
        let socket = self.ensure_rpc_req()?;
        socket.send(payload, 0).map_err(|e| e.to_string())?;
        let mut poll_items = [socket.as_poll_item(zmq::POLLIN)];
        let timeout_ms = (timeout * 1000.0).max(1.0) as i64;
        zmq::poll(&mut poll_items, timeout_ms).map_err(|e| e.to_string())?;
        if !poll_items[0].is_readable() {
            return Err(format!("RPC调用 {} 超时（{}s）", event_type, timeout));
        }
        let response_payload = socket.recv_bytes(0).map_err(|e| e.to_string())?;
        let response = deserialize_response(&response_payload)?;
        if let Some(error) = response.error {
            return Err(format!("RPC Remote Error: {}", error));
        }
        Ok(response.data.unwrap_or(Value::Null))
    }

    fn bind_socket(
        context: &zmq::Context,
        socket_type: zmq::SocketType,
        endpoint: &str,
    ) -> Result<(zmq::Socket, String), String> {
        let socket = context.socket(socket_type).map_err(|e| e.to_string())?;
        match socket.bind(endpoint) {
            Ok(_) => Ok((socket, endpoint.to_string())),
            Err(e) => {
                if e == zmq::Error::EADDRINUSE && endpoint.starts_with("tcp://") {
                    let fallback = "tcp://127.0.0.1:*";
                    socket.bind(fallback).map_err(|e| e.to_string())?;
                    let actual = socket
                        .get_last_endpoint()
                        .map_err(|e| e.to_string())?
                        .map_err(|e| String::from_utf8_lossy(&e).to_string())?;
                    Ok((socket, actual))
                } else {
                    Err(e.to_string())
                }
            }
        }
    }

    pub fn bind_server_sockets(&mut self) -> Result<(zmq::Socket, zmq::Socket), String> {
        if self.redis_app.is_some() {
            return Err("redis backend active".to_string());
        }
        let (event_pull, event_endpoint) =
            Self::bind_socket(&self.context, zmq::PULL, &self.event_endpoint)?;
        let (rpc_rep, rpc_endpoint) =
            Self::bind_socket(&self.context, zmq::REP, &self.rpc_endpoint)?;
        self.event_endpoint = event_endpoint;
        self.rpc_endpoint = rpc_endpoint;
        Ok((event_pull, rpc_rep))
    }

    pub fn run_with_sockets(
        &mut self,
        event_pull: zmq::Socket,
        rpc_rep: zmq::Socket,
    ) -> Result<(), String> {
        if self.redis_app.is_some() {
            return Err("redis backend active".to_string());
        }
        let mut poll_items = [
            event_pull.as_poll_item(zmq::POLLIN),
            rpc_rep.as_poll_item(zmq::POLLIN),
        ];
        loop {
            zmq::poll(&mut poll_items, 1000).map_err(|e| e.to_string())?;
            if poll_items[0].is_readable() {
                let payload = event_pull.recv_bytes(0).map_err(|e| e.to_string())?;
                if let Ok(message) = deserialize_message(&payload) {
                    if let Some(handlers) = self.subscribers.get(&message.event_type) {
                        for handler in handlers {
                            handler(message.data.clone());
                        }
                    }
                }
            }
            if poll_items[1].is_readable() {
                let payload = rpc_rep.recv_bytes(0).map_err(|e| e.to_string())?;
                let response = match deserialize_message(&payload) {
                    Ok(message) => {
                        if let Some(handler) = self.rpc_handlers.get(&message.event_type) {
                            match handler(message.data) {
                                Ok(result) => RpcResponse {
                                    request_id: message.request_id,
                                    data: Some(result),
                                    error: None,
                                },
                                Err(error) => RpcResponse {
                                    request_id: message.request_id,
                                    data: None,
                                    error: Some(error),
                                },
                            }
                        } else {
                            RpcResponse {
                                request_id: message.request_id,
                                data: None,
                                error: Some("handler not found".to_string()),
                            }
                        }
                    }
                    Err(error) => RpcResponse {
                        request_id: None,
                        data: None,
                        error: Some(error),
                    },
                };
                let response_payload = serialize_response(&response)?;
                rpc_rep
                    .send(response_payload, 0)
                    .map_err(|e| e.to_string())?;
            }
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        if let Some(app) = self.redis_app.as_mut() {
            return app.run();
        }
        let (event_pull, rpc_rep) = self.bind_server_sockets()?;
        self.run_with_sockets(event_pull, rpc_rep)
    }
}
