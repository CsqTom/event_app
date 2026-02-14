import time
import threading
import queue
from typing import Optional

from pydantic import BaseModel

from event_app_redis import EventApp

class ImageFrame(BaseModel):
    id: str
    frame_id: int
    width: int
    height: int
    format: str
    data: str
    processed: Optional[bool] = None


def run_client():
    app = EventApp(group_name="video_stream_client")
    counters = {"received": 0}
    log_queue: queue.SimpleQueue = queue.SimpleQueue()

    def log_worker():
        while True:
            message = log_queue.get()
            print(message)

    logger_thread = threading.Thread(target=log_worker, daemon=True)
    logger_thread.start()

    @app.subscribe("image_frame", ImageFrame)
    def on_frame(data):
        counters["received"] += 1
        log_queue.put(f"{time.time()} on_frame -> {counters['received']}")
        if isinstance(data, dict):
            data["processed"] = True
            app.publish("image_frame_processed", data)

    listener_thread = threading.Thread(target=app._listen_loop, daemon=True)
    listener_thread.start()

    time.sleep(1)
    open_result = app.get(
        "stream_open",
        {"id": "stream-001", "rtmp_url": "rtmp://127.0.0.1:1935/live/test_stream", "rtmp_result_url": "rtmp://127.0.0.1:1935/live/test_stream_out"},
        timeout=5.0,
    )
    print(f"stream_open -> {open_result}")

    time.sleep(30)
    close_result = app.get("stream_close", {"id": "stream-001"}, timeout=5.0)
    print(f"stream_close -> {close_result}")

    time.sleep(0.5)
    print(f"received_frames -> {counters['received']}")


if __name__ == "__main__":
    run_client()
