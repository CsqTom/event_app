use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::event_app_redis::{serialize_publish_data, stream_key, RedisConfig};

pub struct StreamTaskManager {
    #[allow(dead_code)]
    redis_config: RedisConfig,
    payload_b64: String,
    tasks: HashMap<String, StreamTask>,
    processed_frame_count: u64,
    redis_client: redis::Client,
}

struct StreamTask {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

impl StreamTaskManager {
    pub fn new(redis_config: RedisConfig, payload: Vec<u8>) -> Result<Self, String> {
        let payload_b64 = STANDARD.encode(payload);
        let redis_client = redis::Client::open(redis_config.to_url()).map_err(|e| e.to_string())?;
        Ok(Self {
            redis_config,
            payload_b64,
            tasks: HashMap::new(),
            processed_frame_count: 0,
            redis_client,
        })
    }

    pub fn open(&mut self, data: &Value) -> Value {
        let stream_id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let rtmp_url = data.get("rtmp_url").and_then(|v| v.as_str()).unwrap_or("");
        let rtmp_result_url = data
            .get("rtmp_result_url")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if stream_id.is_empty() || rtmp_url.is_empty() || rtmp_result_url.is_empty() {
            return json!({"status": "error", "error": "missing id/rtmp_url/rtmp_result_url"});
        }
        if self.tasks.contains_key(stream_id) {
            return json!({"status": "opened", "id": stream_id});
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let payload_b64 = self.payload_b64.clone();
        let client = self.redis_client.clone();
        let stream_id_string = stream_id.to_string();
        let handle = thread::spawn(move || {
            let mut conn = match client.get_connection() {
                Ok(conn) => conn,
                Err(_) => return,
            };
            let key = stream_key("image_frame");
            let mut frame_id: u64 = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                let data = json!({
                    "id": stream_id_string,
                    "frame_id": frame_id,
                    "data": payload_b64,
                });
                if let Ok(payload) = serialize_publish_data(data) {
                    let _ = redis::cmd("XADD")
                        .arg(&key)
                        .arg("*")
                        .arg("payload")
                        .arg(payload)
                        .query::<()>(&mut conn);
                }
                frame_id += 1;
                thread::sleep(Duration::from_millis(100));
            }
        });
        self.tasks.insert(
            stream_id.to_string(),
            StreamTask {
                stop,
                handle,
            },
        );
        json!({"status": "opened", "id": stream_id})
    }

    pub fn close(&mut self, data: &Value) -> Value {
        let stream_id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if stream_id.is_empty() {
            return json!({"status": "error", "error": "missing id"});
        }
        if let Some(task) = self.tasks.remove(stream_id) {
            task.stop.store(true, Ordering::Relaxed);
            let _ = task.handle.join();
        }
        json!({"status": "closed", "id": stream_id})
    }

    pub fn on_processed_frame(&mut self, data: &Value) {
        let stream_id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let frame_id = data.get("frame_id").and_then(|v| v.as_u64()).unwrap_or(0);
        self.processed_frame_count += 1;
        println!(
            "push_stream placeholder: id={}, frame_id={}, processed_count={}",
            stream_id, frame_id, self.processed_frame_count
        );
    }
}
