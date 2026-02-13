mod event_app_redis;
mod stream_task_manager;

use event_app_redis::{EventAppRedis, RedisConfig};
use serde_json::Value;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use stream_task_manager::StreamTaskManager;

fn main() -> Result<(), String> {
    let config = RedisConfig::default();
    let mut app = EventAppRedis::new(Some(config.clone()), Some("video_stream_server".to_string()))?;
    let manager = Arc::new(Mutex::new(StreamTaskManager::new(config)?));

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

    let (processed_tx, processed_rx) = mpsc::channel::<Value>();
    let manager_processed = manager.clone();
    thread::spawn(move || {
        while let Ok(data) = processed_rx.recv() {
            if let Ok(mut mgr) = manager_processed.lock() {
                mgr.on_processed_frame(&data);
            }
        }
    });

    app.subscribe("image_frame_processed", move |data: Value| {
        let _ = processed_tx.send(data);
    });

    app.run()
}
