use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ffmpeg_next as ffmpeg;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::event_app_redis::{channel_key, serialize_publish_data, RedisConfig};
use serde::ser::Serializer;
use serde::Serialize;

pub struct StreamTaskManager {
    #[allow(dead_code)]
    redis_config: RedisConfig,
    tasks: HashMap<String, StreamTask>,
    processed_frame_count: u64,
    redis_client: redis::Client,
    statuses: Arc<Mutex<HashMap<String, StreamStatus>>>,
    output_tasks: HashMap<String, OutputTask>,
    processing_ids: Arc<Mutex<HashMap<String, usize>>>,
}

struct StreamTask {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

struct OutputTask {
    stop: Arc<AtomicBool>,
    sender: mpsc::Sender<ProcessedFrame>,
    handle: thread::JoinHandle<()>,
}

struct ProcessedFrame {
    width: usize,
    height: usize,
    data: Vec<u8>,
}

struct OutputSession {
    width: usize,
    height: usize,
    octx: ffmpeg::format::context::Output,
    encoder: ffmpeg::encoder::video::Encoder,
    scaler: ffmpeg::software::scaling::context::Context,
    ost_index: usize,
    time_base: ffmpeg::Rational,
    next_pts: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamStatus {
    Connecting,
    Running,
    Reconnecting,
    Closed,
    Failed,
}

enum SessionOutcome {
    Stopped,
    Ended,
}

impl StreamTaskManager {
    pub fn new(redis_config: RedisConfig) -> Result<Self, String> {
        static FFMPEG_INIT: Once = Once::new();
        let mut init_error: Option<String> = None;
        FFMPEG_INIT.call_once(|| {
            if let Err(err) = ffmpeg::init() {
                init_error = Some(err.to_string());
            }
        });
        if let Some(err) = init_error {
            return Err(err);
        }
        let redis_client = redis::Client::open(redis_config.to_url()).map_err(|e| e.to_string())?;
        Ok(Self {
            redis_config,
            tasks: HashMap::new(),
            processed_frame_count: 0,
            redis_client,
            statuses: Arc::new(Mutex::new(HashMap::new())),
            output_tasks: HashMap::new(),
            processing_ids: Arc::new(Mutex::new(HashMap::new())),
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
        let client = self.redis_client.clone();
        let statuses = self.statuses.clone();
        let stream_id_string = stream_id.to_string();
        let input_url = rtmp_url.to_string();
        let output_url = rtmp_result_url.to_string();
        log_ts(&format!(
            "stream_open received id={} rtmp_url={} rtmp_result_url={}",
            stream_id_string, input_url, output_url
        ));
        if !self.output_tasks.contains_key(&stream_id_string) {
            let (sender, receiver) = mpsc::channel::<ProcessedFrame>();
            let output_stop = Arc::new(AtomicBool::new(false));
            let output_stop_flag = output_stop.clone();
            let output_id = stream_id_string.clone();
            let output_client = self.redis_client.clone();
            let output_processing_ids = self.processing_ids.clone();
            let output_handle = thread::spawn(move || {
                let mut session: Option<OutputSession> = None;
                loop {
                    if output_stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    match receiver.recv_timeout(Duration::from_millis(200)) {
                        Ok(frame) => {
                            if session.is_none() {
                                log_ts(&format!(
                                    "rtmp_output init id={} url={} size={}x{}",
                                    output_id, output_url, frame.width, frame.height
                                ));
                                match create_output_session(&output_url, frame.width, frame.height)
                                {
                                    Ok(created) => session = Some(created),
                                    Err(err) => {
                                        log_ts(&format!(
                                            "rtmp_output init failed id={} err={}",
                                            output_id, err
                                        ));
                                        publish_error_event(&output_client, &output_id, &err);
                                        continue;
                                    }
                                }
                            }
                            if let Some(active) = session.as_mut() {
                                if let Err(err) = write_frame_to_output(active, &frame.data) {
                                    log_ts(&format!(
                                        "rtmp_output write failed id={} err={}",
                                        output_id, err
                                    ));
                                    publish_error_event(&output_client, &output_id, &err);
                                }
                            }
                            finish_processing(&output_processing_ids, &output_id);
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                if let Some(active) = session {
                    let _ = close_output_session(active);
                }
            });
            self.output_tasks.insert(
                stream_id_string.clone(),
                OutputTask {
                    stop: output_stop,
                    sender,
                    handle: output_handle,
                },
            );
        }
        update_status(&statuses, &stream_id_string, StreamStatus::Connecting);
        let handle = thread::spawn(move || {
            let reconnect_window = Duration::from_secs(20);
            let reconnect_interval = Duration::from_secs(2);
            let start = Instant::now();
            let mut attempt = 0;
            loop {
                if stop_flag.load(Ordering::Relaxed) {
                    update_status(&statuses, &stream_id_string, StreamStatus::Closed);
                    break;
                }
                let status = if attempt == 0 {
                    StreamStatus::Connecting
                } else {
                    StreamStatus::Reconnecting
                };
                update_status(&statuses, &stream_id_string, status);
                let mut conn = match client.get_connection() {
                    Ok(conn) => conn,
                    Err(err) => {
                        if start.elapsed() >= reconnect_window {
                            update_status(&statuses, &stream_id_string, StreamStatus::Failed);
                            publish_error_event(&client, &stream_id_string, &err.to_string());
                            break;
                        }
                        attempt += 1;
                        thread::sleep(reconnect_interval);
                        continue;
                    }
                };
                match run_stream_session(
                    &stream_id_string,
                    &input_url,
                    &mut conn,
                    &stop_flag,
                    &statuses,
                ) {
                    Ok(SessionOutcome::Stopped) => {
                        update_status(&statuses, &stream_id_string, StreamStatus::Closed);
                        break;
                    }
                    Ok(SessionOutcome::Ended) => {
                        if start.elapsed() >= reconnect_window {
                            update_status(&statuses, &stream_id_string, StreamStatus::Failed);
                            publish_error_event(&client, &stream_id_string, "stream ended");
                            break;
                        }
                    }
                    Err(err) => {
                        if start.elapsed() >= reconnect_window {
                            update_status(&statuses, &stream_id_string, StreamStatus::Failed);
                            publish_error_event(&client, &stream_id_string, &err);
                            break;
                        }
                    }
                }
                attempt += 1;
                thread::sleep(reconnect_interval);
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
        log_ts(&format!("stream_close received id={}", stream_id));
        if let Some(task) = self.tasks.remove(stream_id) {
            task.stop.store(true, Ordering::Relaxed);
            thread::spawn(move || {
                let _ = task.handle.join();
            });
        }
        update_status(&self.statuses, stream_id, StreamStatus::Closed);
        if let Some(task) = self.output_tasks.remove(stream_id) {
            task.stop.store(true, Ordering::Relaxed);
            drop(task.sender);
            thread::spawn(move || {
                let _ = task.handle.join();
            });
        }
        clear_processing(&self.processing_ids, stream_id);
        json!({"status": "closed", "id": stream_id})
    }

    pub fn on_processed_frame(&mut self, data: &Value) {
        let stream_id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if stream_id.is_empty() {
            return;
        }
        let status = self
            .statuses
            .lock()
            .ok()
            .and_then(|map| map.get(stream_id).copied());
        if status != Some(StreamStatus::Running) {
            return;
        }
        let frame_id = data.get("frame_id").and_then(|v| v.as_u64()).unwrap_or(0);
        let width = data.get("width").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let height = data.get("height").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let format = data.get("format").and_then(|v| v.as_str()).unwrap_or("");
        let payload_b64 = data.get("data").and_then(|v| v.as_str()).unwrap_or("");
        if width == 0 || height == 0 || format != "rgb24" || payload_b64.is_empty() {
            return;
        }
        let Ok(raw) = STANDARD.decode(payload_b64) else {
            publish_error_event(&self.redis_client, stream_id, "decode processed frame failed");
            return;
        };
        if let Some(task) = self.output_tasks.get(stream_id) {
            start_processing(&self.processing_ids, stream_id);
            let _ = task.sender.send(ProcessedFrame {
                width,
                height,
                data: raw,
            });
            self.processed_frame_count += 1;
            log_ts(&format!(
                "push_stream enqueue id={} frame_id={} processed_count={}",
                stream_id, frame_id, self.processed_frame_count
            ));
        }
    }
}

fn start_processing(
    processing_ids: &Arc<Mutex<HashMap<String, usize>>>,
    stream_id: &str,
) {
    if let Ok(mut map) = processing_ids.lock() {
        let count = map.entry(stream_id.to_string()).or_insert(0);
        *count += 1;
    }
}

fn finish_processing(
    processing_ids: &Arc<Mutex<HashMap<String, usize>>>,
    stream_id: &str,
) {
    if let Ok(mut map) = processing_ids.lock() {
        if let Some(count) = map.get_mut(stream_id) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                map.remove(stream_id);
            }
        }
    }
}

fn clear_processing(processing_ids: &Arc<Mutex<HashMap<String, usize>>>, stream_id: &str) {
    if let Ok(mut map) = processing_ids.lock() {
        map.remove(stream_id);
    }
}

fn update_status(
    statuses: &Arc<Mutex<HashMap<String, StreamStatus>>>,
    stream_id: &str,
    status: StreamStatus,
) {
    if let Ok(mut map) = statuses.lock() {
        map.insert(stream_id.to_string(), status);
    }
}

fn publish_error_event(client: &redis::Client, stream_id: &str, error: &str) {
    if let Ok(mut conn) = client.get_connection() {
        let data = json!({
            "id": stream_id,
            "status": "failed",
            "error": error
        });
        if let Ok(payload) = serialize_publish_data(data) {
            let key = channel_key("stream_error");
            let _ = redis::cmd("PUBLISH")
                .arg(&key)
                .arg(payload)
                .query::<()>(&mut conn);
        }
    }
}

fn publish_frame_event(
    conn: &mut redis::Connection,
    stream_id: &str,
    frame_id: u64,
    width: usize,
    height: usize,
    data: Vec<u8>,
) {
    log_ts(&format!(
        "publish_frame id={} frame_id={} size={}x{}",
        stream_id, frame_id, width, height
    ));
    let payload_data = PublishPayload {
        data: FramePayload {
            id: stream_id.to_string(),
            frame_id,
            width,
            height,
            format: "rgb24".to_string(),
            data: PickleBytes(data),
        },
        need_response: false,
        request_id: None,
    };
    if let Ok(payload) = serde_pickle::to_vec(&payload_data, serde_pickle::SerOptions::new()) {
        let key = channel_key("image_frame");
        let _ = redis::cmd("PUBLISH")
            .arg(&key)
            .arg(payload)
            .query::<()>(&mut *conn);
    }
}

struct PickleBytes(Vec<u8>);

impl Serialize for PickleBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

#[derive(Serialize)]
struct FramePayload {
    id: String,
    frame_id: u64,
    width: usize,
    height: usize,
    format: String,
    data: PickleBytes,
}

#[derive(Serialize)]
struct PublishPayload {
    data: FramePayload,
    need_response: bool,
    request_id: Option<String>,
}

fn run_stream_session(
    stream_id: &str,
    input_url: &str,
    conn: &mut redis::Connection,
    stop_flag: &Arc<AtomicBool>,
    statuses: &Arc<Mutex<HashMap<String, StreamStatus>>>,
) -> Result<SessionOutcome, String> {
    log_ts(&format!(
        "rtmp_input start id={} url={}",
        stream_id, input_url
    ));
    let mut ictx = ffmpeg::format::input(&input_url).map_err(|e| e.to_string())?;
    let input_video = ictx
        .streams()
        .best(ffmpeg::media::Type::Video)
        .ok_or_else(|| "no video stream".to_string())?;
    let video_stream_index = input_video.index();

    let mut decoder = ffmpeg::codec::context::Context::from_parameters(input_video.parameters())
        .map_err(|e| e.to_string())?
        .decoder()
        .video()
        .map_err(|e| e.to_string())?;

    let mut scaler = ffmpeg::software::scaling::context::Context::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        ffmpeg::format::pixel::Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        ffmpeg::software::scaling::flag::Flags::BILINEAR,
    )
    .map_err(|e| e.to_string())?;

    update_status(statuses, stream_id, StreamStatus::Running);

    let mut frame = ffmpeg::util::frame::video::Video::empty();
    let mut rgb_frame = ffmpeg::util::frame::video::Video::empty();
    let mut frame_id: u64 = 0;

    for (stream, packet) in ictx.packets() {
        if stop_flag.load(Ordering::Relaxed) {
            return Ok(SessionOutcome::Stopped);
        }
        if stream.index() == video_stream_index {
            if decoder.send_packet(&packet).is_ok() {
                while decoder.receive_frame(&mut frame).is_ok() {
                    if stop_flag.load(Ordering::Relaxed) {
                        return Ok(SessionOutcome::Stopped);
                    }
                    scaler.run(&frame, &mut rgb_frame).map_err(|e| e.to_string())?;
                    let width = rgb_frame.width() as usize;
                    let height = rgb_frame.height() as usize;
                    let stride = rgb_frame.stride(0);
                    let data = rgb_frame.data(0);
                    let mut raw = Vec::with_capacity(width * height * 3);
                    if stride > 0 {
                        let stride = stride as usize;
                        for row in 0..height {
                            let start = row * stride;
                            let end = start + width * 3;
                            if end <= data.len() {
                                raw.extend_from_slice(&data[start..end]);
                            }
                        }
                    }
                    publish_frame_event(conn, stream_id, frame_id, width, height, raw);
                    frame_id += 1;
                }
            }
        }
    }

    decoder.send_eof().ok();
    while decoder.receive_frame(&mut frame).is_ok() {
        if stop_flag.load(Ordering::Relaxed) {
            return Ok(SessionOutcome::Stopped);
        }
        scaler.run(&frame, &mut rgb_frame).map_err(|e| e.to_string())?;
        let width = rgb_frame.width() as usize;
        let height = rgb_frame.height() as usize;
        let stride = rgb_frame.stride(0);
        let data = rgb_frame.data(0);
        let mut raw = Vec::with_capacity(width * height * 3);
        if stride > 0 {
            let stride = stride as usize;
            for row in 0..height {
                let start = row * stride;
                let end = start + width * 3;
                if end <= data.len() {
                    raw.extend_from_slice(&data[start..end]);
                }
            }
        }
        publish_frame_event(conn, stream_id, frame_id, width, height, raw);
        frame_id += 1;
    }
    Ok(SessionOutcome::Ended)
}

fn create_output_session(
    output_url: &str,
    width: usize,
    height: usize,
) -> Result<OutputSession, String> {
    log_ts(&format!(
        "rtmp_output start url={} size={}x{}",
        output_url, width, height
    ));
    let mut octx =
        ffmpeg::format::output_as(output_url, "flv").map_err(|e| e.to_string())?;
    let use_global_header = octx
        .format()
        .flags()
        .contains(ffmpeg::format::Flags::GLOBAL_HEADER);
    let codec = ffmpeg::encoder::find(ffmpeg::codec::Id::H264)
        .ok_or_else(|| "H264 encoder not found".to_string())?;
    let context = ffmpeg::codec::context::Context::new_with_codec(codec);
    let mut encoder = context
        .encoder()
        .video()
        .map_err(|e| e.to_string())?;
    encoder.set_width(width as u32);
    encoder.set_height(height as u32);
    encoder.set_format(ffmpeg::format::pixel::Pixel::YUV420P);
    encoder.set_time_base(ffmpeg::Rational(1, 25));
    encoder.set_frame_rate(Some(ffmpeg::Rational(25, 1)));
    if use_global_header {
        encoder.set_flags(ffmpeg::codec::Flags::GLOBAL_HEADER);
    }
    let mut opts = ffmpeg::Dictionary::new();
    opts.set("preset", "veryfast");
    opts.set("tune", "zerolatency");
    let encoder = encoder.open_with(opts).map_err(|e| e.to_string())?;
    let ost_index = {
        let mut ostream = octx.add_stream(codec).map_err(|e| e.to_string())?;
        ostream.set_parameters(&encoder);
        ostream.index()
    };
    let mut format_opts = ffmpeg::Dictionary::new();
    format_opts.set("flvflags", "no_duration_filesize");
    octx.write_header_with(format_opts).map_err(|e| e.to_string())?;
    let scaler = ffmpeg::software::scaling::context::Context::get(
        ffmpeg::format::pixel::Pixel::RGB24,
        width as u32,
        height as u32,
        ffmpeg::format::pixel::Pixel::YUV420P,
        width as u32,
        height as u32,
        ffmpeg::software::scaling::flag::Flags::BILINEAR,
    )
    .map_err(|e| e.to_string())?;
    Ok(OutputSession {
        width,
        height,
        octx,
        encoder,
        scaler,
        ost_index,
        time_base: ffmpeg::Rational(1, 25),
        next_pts: 0,
    })
}

fn write_frame_to_output(session: &mut OutputSession, rgb_data: &[u8]) -> Result<(), String> {
    let expected = session.width * session.height * 3;
    if rgb_data.len() < expected {
        return Err("processed frame size mismatch".to_string());
    }
    let mut rgb_frame = ffmpeg::util::frame::video::Video::new(
        ffmpeg::format::pixel::Pixel::RGB24,
        session.width as u32,
        session.height as u32,
    );
    let stride = rgb_frame.stride(0);
    let dst = rgb_frame.data_mut(0);
    let row_bytes = session.width * 3;
    for row in 0..session.height {
        let src_start = row * row_bytes;
        let dst_start = row * stride;
        let src = &rgb_data[src_start..src_start + row_bytes];
        let dst_row = &mut dst[dst_start..dst_start + row_bytes];
        dst_row.copy_from_slice(src);
    }
    let mut yuv_frame = ffmpeg::util::frame::video::Video::new(
        ffmpeg::format::pixel::Pixel::YUV420P,
        session.width as u32,
        session.height as u32,
    );
    session
        .scaler
        .run(&rgb_frame, &mut yuv_frame)
        .map_err(|e| e.to_string())?;
    yuv_frame.set_pts(Some(session.next_pts));
    session.next_pts += 1;
    session
        .encoder
        .send_frame(&yuv_frame)
        .map_err(|e| e.to_string())?;
    let mut encoded = ffmpeg::Packet::empty();
    while session.encoder.receive_packet(&mut encoded).is_ok() {
        encoded.set_stream(session.ost_index);
        encoded.rescale_ts(session.time_base, session.octx.stream(session.ost_index).unwrap().time_base());
        encoded
            .write_interleaved(&mut session.octx)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn close_output_session(mut session: OutputSession) -> Result<(), String> {
    log_ts("rtmp_output closing");
    session
        .encoder
        .send_eof()
        .map_err(|e| e.to_string())?;
    let mut encoded = ffmpeg::Packet::empty();
    while session.encoder.receive_packet(&mut encoded).is_ok() {
        encoded.set_stream(session.ost_index);
        encoded.rescale_ts(session.time_base, session.octx.stream(session.ost_index).unwrap().time_base());
        encoded
            .write_interleaved(&mut session.octx)
            .map_err(|e| e.to_string())?;
    }
    session.octx.write_trailer().map_err(|e| e.to_string())
}

fn log_ts(message: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    println!("[{}.{:03}] {}", secs, millis, message);
}
