mod event_app_redis;
mod stream_task_manager;

use event_app_redis::{EventAppRedis, RedisConfig};
use serde_json::Value;
use std::fs;
use std::sync::{Arc, Mutex};
use stream_task_manager::StreamTaskManager;

fn load_image_bytes() -> Result<Vec<u8>, String> {
    let base = std::env::current_dir().map_err(|e| e.to_string())?;
    let path = base.join("..").join("260203090900688_frame2225.jpg");
    fs::read(path).map_err(|e| e.to_string())
}

fn main() -> Result<(), String> {
    let config = RedisConfig::default();
    let payload = load_image_bytes()?;
    let mut app = EventAppRedis::new(Some(config.clone()), Some("video_stream_server".to_string()))?;
    let manager = Arc::new(Mutex::new(StreamTaskManager::new(
        config,
        payload,
    )?));

    let manager_open = manager.clone();
    app.rpc("stream_open", move |data: Value| {
        let mut mgr = manager_open.lock().map_err(|_| "lock error".to_string())?;
        Ok(mgr.open(&data))
    });

    let manager_close = manager.clone();
    app.rpc("stream_close", move |data: Value| {
        let mut mgr = manager_close.lock().map_err(|_| "lock error".to_string())?;
        Ok(mgr.close(&data))
    });

    let manager_processed = manager.clone();
    app.subscribe("image_frame_processed", move |data: Value| {
        let mut mgr = manager_processed.lock().unwrap_or_else(|e| e.into_inner());
        mgr.on_processed_frame(&data);
    });

    app.run()
}
