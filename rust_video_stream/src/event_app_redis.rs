use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use redis::streams::StreamReadReply;
use serde_json::{json, Value};
use serde_pickle::value::{HashableValue, Value as PickleValue};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    pub db: i64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
        }
    }
}

impl RedisConfig {
    pub fn to_url(&self) -> String {
        format!("redis://{}:{}/{}", self.host, self.port, self.db)
    }
}

type SubHandler = Box<dyn Fn(Value) + Send + Sync>;
type RpcHandler = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

pub struct EventAppRedis {
    #[allow(dead_code)]
    config: RedisConfig,
    group_name: String,
    subscribers: HashMap<String, Vec<SubHandler>>,
    rpc_handlers: HashMap<String, RpcHandler>,
    client: redis::Client,
}

pub fn stream_key(event_type: &str) -> String {
    format!("event:{}:stream", event_type)
}

pub fn channel_key(event_type: &str) -> String {
    format!("event:{}:channel", event_type)
}

fn event_type_from_channel(channel: &str) -> &str {
    let prefix = "event:";
    let suffix = ":channel";
    if channel.starts_with(prefix) && channel.ends_with(suffix) {
        return &channel[prefix.len()..channel.len() - suffix.len()];
    }
    channel
}

fn is_timeout_error(err: &redis::RedisError) -> bool {
    if err.kind() != redis::ErrorKind::IoError {
        return false;
    }
    let message = err.to_string().to_lowercase();
    message.contains("timed out")
        || message.contains("timeout")
        || message.contains("10060")
        || message.contains("connection attempt failed")
}

fn is_misconf_error(err: &redis::RedisError) -> bool {
    err.to_string().contains("MISCONF")
}

pub fn serialize_data(data: &Value) -> Result<Vec<u8>, String> {
    serde_pickle::to_vec(data, serde_pickle::SerOptions::new()).map_err(|e| e.to_string())
}

pub fn deserialize_data(payload: &[u8]) -> Result<Value, String> {
    let value: PickleValue =
        serde_pickle::from_slice(payload, serde_pickle::DeOptions::new())
            .map_err(|e| e.to_string())?;
    Ok(pickle_to_json(value))
}

pub fn serialize_publish_data(data: Value) -> Result<Vec<u8>, String> {
    let payload = json!({
        "data": data,
        "need_response": false,
        "request_id": Value::Null
    });
    serialize_data(&payload)
}

impl EventAppRedis {
    pub fn new(config: Option<RedisConfig>, group_name: Option<String>) -> Result<Self, String> {
        let config = config.unwrap_or_default();
        let client = redis::Client::open(config.to_url()).map_err(|e| e.to_string())?;
        Ok(Self {
            config,
            group_name: group_name.unwrap_or_else(|| "event_app_group".to_string()),
            subscribers: HashMap::new(),
            rpc_handlers: HashMap::new(),
            client,
        })
    }

    pub fn subscribe<F>(&mut self, event_type: &str, handler: F)
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        let entry = self.subscribers.entry(event_type.to_string()).or_default();
        entry.push(Box::new(handler));
    }

    pub fn rpc<F>(&mut self, event_type: &str, handler: F)
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.rpc_handlers
            .insert(event_type.to_string(), Box::new(handler));
    }

    fn connection(&self) -> Result<redis::Connection, String> {
        self.client.get_connection().map_err(|e| e.to_string())
    }

    fn ensure_group(
        &self,
        conn: &mut redis::Connection,
        stream: &str,
        start_id: &str,
        reset_last_id: bool,
    ) -> Result<(), String> {
        let result: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(&self.group_name)
            .arg(start_id)
            .arg("MKSTREAM")
            .query(conn);
        if let Err(err) = result {
            if is_misconf_error(&err) {
                let _ = redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("stop-writes-on-bgsave-error")
                    .arg("no")
                    .query::<()>(&mut *conn);
                let retry: Result<(), redis::RedisError> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(stream)
                    .arg(&self.group_name)
                    .arg(start_id)
                    .arg("MKSTREAM")
                    .query(conn);
                if let Err(retry_err) = retry {
                    let msg = retry_err.to_string();
                    if !msg.contains("BUSYGROUP") {
                        return Err(msg);
                    }
                } else {
                    return Ok(());
                }
            } else {
                let msg = err.to_string();
                if !msg.contains("BUSYGROUP") {
                    return Err(msg);
                }
            }
            if reset_last_id {
                self.exec_with_retry(conn, |conn| {
                    redis::cmd("XGROUP")
                        .arg("SETID")
                        .arg(stream)
                        .arg(&self.group_name)
                        .arg(start_id)
                        .query::<()>(conn)
                })?;
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn publish(&self, event_type: &str, data: Value) -> Result<(), String> {
        let mut conn = self.connection()?;
        let payload = serialize_publish_data(data)?;
        let channel = channel_key(event_type);
        self.exec_with_retry(&mut conn, |conn| {
            redis::cmd("PUBLISH")
                .arg(channel.clone())
                .arg(payload.clone())
                .query::<()>(conn)
        })
    }

    #[allow(dead_code)]
    pub fn get(&self, event_type: &str, data: Value, timeout: f64) -> Result<Value, String> {
        let mut conn = self.connection()?;
        let request_id = Uuid::new_v4().to_string();
        let response_key = format!("event:response:{}", request_id);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs_f64();
        let payload = json!({
            "data": data,
            "need_response": true,
            "request_id": request_id,
            "reply_to": response_key,
            "timestamp": timestamp,
            "timeout": timeout
        });
        let stream = stream_key(event_type);
        let payload_bytes = serialize_data(&payload)?;
        self.exec_with_retry(&mut conn, |conn| {
            redis::cmd("XADD")
                .arg(stream.clone())
                .arg("*")
                .arg("payload")
                .arg(payload_bytes.clone())
                .query::<()>(conn)
        })?;

        let blpop_timeout = if timeout >= 1.0 { timeout as usize } else { 1 };
        let result: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
            .arg(&response_key)
            .arg(blpop_timeout)
            .query(&mut conn)
            .map_err(|e| e.to_string())?;
        self.exec_with_retry(&mut conn, |conn| {
            redis::cmd("DEL").arg(&response_key).query::<()>(conn)
        })?;
        let payload = match result {
            Some((_, payload)) => payload,
            None => {
                return Err(format!("RPC调用 {} 超时（{}s）", event_type, timeout));
            }
        };
        let response = deserialize_data(&payload)?;
        if let Some(error) = response.get("error").and_then(|v| v.as_str()) {
            return Err(format!("RPC Remote Error: {}", error));
        }
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    }

    pub fn run(&mut self) -> Result<(), String> {
        let mut stream_conn = self.connection()?;
        let mut pubsub_conn = if self.subscribers.is_empty() {
            None
        } else {
            Some(self.connection()?)
        };
        if let Some(conn) = pubsub_conn.as_mut() {
            conn.set_read_timeout(Some(Duration::from_millis(50)))
                .map_err(|e| e.to_string())?;
        }
        let mut pubsub = pubsub_conn.as_mut().map(|conn| conn.as_pubsub());
        let consumer = format!("consumer-{}", Uuid::new_v4());
        let mut stream_keys: Vec<String> =
            self.rpc_handlers.keys().map(|s| stream_key(s)).collect();
        stream_keys.sort();
        stream_keys.dedup();
        if stream_keys.is_empty() && pubsub.is_none() {
            return Err("no subscribed events".to_string());
        }
        if let Some(pubsub) = pubsub.as_mut() {
            let mut channel_keys: Vec<String> = self
                .subscribers
                .keys()
                .map(|s| channel_key(s))
                .collect();
            channel_keys.sort();
            channel_keys.dedup();
            for channel in channel_keys {
                pubsub.subscribe(channel).map_err(|e| e.to_string())?;
            }
        }
        for key in &stream_keys {
            self.ensure_group(&mut stream_conn, key, "$", true)?;
        }
        loop {
            if let Some(pubsub) = pubsub.as_mut() {
                match pubsub.get_message() {
                    Ok(message) => {
                        let channel = message.get_channel_name();
                        let event_type = event_type_from_channel(channel);
                        let payload: Vec<u8> = match message.get_payload() {
                            Ok(bytes) => bytes,
                            Err(_) => {
                                let text: String =
                                    message.get_payload().map_err(|e| e.to_string())?;
                                text.into_bytes()
                            }
                        };
                        let publish_data = deserialize_data(&payload)?;
                        self.process_event(event_type, publish_data, &mut stream_conn)?;
                    }
                    Err(err) => {
                        if !is_timeout_error(&err) {
                            return Err(err.to_string());
                        }
                    }
                }
            }
            if stream_keys.is_empty() {
                continue;
            }
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(&self.group_name)
                .arg(&consumer)
                .arg("BLOCK")
                .arg(50)
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS");
            for key in &stream_keys {
                cmd.arg(key);
            }
            for _ in &stream_keys {
                cmd.arg(">");
            }
            let reply: Option<StreamReadReply> =
                cmd.query(&mut stream_conn).map_err(|e| e.to_string())?;
            let Some(reply) = reply else {
                continue;
            };
            for stream in reply.keys {
                let event_type = stream.key.strip_prefix("event:").and_then(|s| s.strip_suffix(":stream")).unwrap_or(&stream.key);
                for id in stream.ids {
                    let payload_value = id.map.get("payload");
                    let payload = match payload_value.and_then(redis_value_to_bytes) {
                        Some(payload) => payload,
                        None => {
                            self.exec_with_retry(&mut stream_conn, |conn| {
                                redis::cmd("XACK")
                                    .arg(&stream.key)
                                    .arg(&self.group_name)
                                    .arg(&id.id)
                                    .query::<()>(conn)
                            })?;
                            self.exec_with_retry(&mut stream_conn, |conn| {
                                redis::cmd("XDEL")
                                    .arg(&stream.key)
                                    .arg(&id.id)
                                    .query::<()>(conn)
                            })?;
                            continue;
                        }
                    };
                    let publish_data = deserialize_data(&payload)?;
                    self.process_event(event_type, publish_data, &mut stream_conn)?;
                    self.exec_with_retry(&mut stream_conn, |conn| {
                        redis::cmd("XACK")
                            .arg(&stream.key)
                            .arg(&self.group_name)
                            .arg(&id.id)
                            .query::<()>(conn)
                    })?;
                    self.exec_with_retry(&mut stream_conn, |conn| {
                        redis::cmd("XDEL")
                            .arg(&stream.key)
                            .arg(&id.id)
                            .query::<()>(conn)
                    })?;
                }
            }
        }
    }

    fn process_event(
        &self,
        event_type: &str,
        publish_data: Value,
        conn: &mut redis::Connection,
    ) -> Result<(), String> {
        let data = publish_data.get("data").cloned().unwrap_or(Value::Null);
        let need_response = publish_data
            .get("need_response")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if need_response {
            let request_id = publish_data
                .get("request_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let reply_to = publish_data
                .get("reply_to")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timestamp = publish_data.get("timestamp").and_then(|v| v.as_f64());
            let timeout = publish_data.get("timeout").and_then(|v| v.as_f64());
            if let (Some(timestamp), Some(timeout)) = (timestamp, timeout) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_secs_f64();
                if now - timestamp > timeout {
                    return Ok(());
                }
            }
            if let Some(handler) = self.rpc_handlers.get(event_type) {
                match handler(data) {
                    Ok(result) => {
                        if let Some(reply_to) = reply_to {
                            let response = json!({"request_id": request_id, "data": result});
                            let payload = serialize_data(&response)?;
                            self.exec_with_retry(conn, |conn| {
                                redis::cmd("RPUSH")
                                    .arg(&reply_to)
                                    .arg(payload.clone())
                                    .query::<()>(conn)
                            })?;
                            self.exec_with_retry(conn, |conn| {
                                redis::cmd("EXPIRE")
                                    .arg(&reply_to)
                                    .arg(60)
                                    .query::<()>(conn)
                            })?;
                        }
                    }
                    Err(error) => {
                        if let Some(reply_to) = reply_to {
                            let response = json!({"request_id": request_id, "error": error});
                            let payload = serialize_data(&response)?;
                            self.exec_with_retry(conn, |conn| {
                                redis::cmd("RPUSH")
                                    .arg(&reply_to)
                                    .arg(payload.clone())
                                    .query::<()>(conn)
                            })?;
                            self.exec_with_retry(conn, |conn| {
                                redis::cmd("EXPIRE")
                                    .arg(&reply_to)
                                    .arg(60)
                                    .query::<()>(conn)
                            })?;
                        }
                    }
                }
            }
        } else if let Some(handlers) = self.subscribers.get(event_type) {
            for handler in handlers {
                handler(data.clone());
            }
        }
        Ok(())
    }

    fn exec_with_retry<T, F>(&self, conn: &mut redis::Connection, mut op: F) -> Result<T, String>
    where
        F: FnMut(&mut redis::Connection) -> Result<T, redis::RedisError>,
    {
        match op(conn) {
            Ok(result) => Ok(result),
            Err(err) => {
                if is_misconf_error(&err) {
                    let _ = redis::cmd("CONFIG")
                        .arg("SET")
                        .arg("stop-writes-on-bgsave-error")
                        .arg("no")
                        .query::<()>(&mut *conn);
                    return op(conn).map_err(|e| e.to_string());
                }
                Err(err.to_string())
            }
        }
    }
}

fn redis_value_to_bytes(value: &redis::Value) -> Option<Vec<u8>> {
    redis::from_redis_value(value).ok()
}

fn pickle_to_json(value: PickleValue) -> Value {
    match value {
        PickleValue::None => Value::Null,
        PickleValue::Bool(v) => Value::Bool(v),
        PickleValue::I64(v) => json!(v),
        PickleValue::F64(v) => json!(v),
        PickleValue::String(s) => Value::String(s),
        PickleValue::Bytes(bytes) => Value::String(STANDARD.encode(bytes)),
        PickleValue::List(items) => {
            Value::Array(items.into_iter().map(pickle_to_json).collect())
        }
        PickleValue::Tuple(items) => {
            Value::Array(items.into_iter().map(pickle_to_json).collect())
        }
        PickleValue::Dict(items) => {
            let mut map = serde_json::Map::new();
            for (k, v) in items {
                let key = pickle_key_to_string(k);
                map.insert(key, pickle_to_json(v));
            }
            Value::Object(map)
        }
        _ => Value::Null,
    }
}

fn pickle_key_to_string(value: HashableValue) -> String {
    match value {
        HashableValue::String(s) => s,
        HashableValue::I64(v) => v.to_string(),
        HashableValue::Bool(v) => v.to_string(),
        HashableValue::F64(v) => v.to_string(),
        HashableValue::None => "null".to_string(),
        _ => "key".to_string(),
    }
}
