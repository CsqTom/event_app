import time
import threading
from q_log.log_nb import logger
from typing import Optional

from pydantic import BaseModel

from event_app_redis import EventApp

class ImageFrame(BaseModel):
    id: str
    frame_id: int
    width: int
    height: int
    format: str
    data: bytes
    processed: Optional[bool] = None


def run_client():
    app = EventApp(
        group_name="video_stream_client",
        max_workers=4,
        batch_size=10,
        block_timeout_ms=50,
    )

    @app.subscribe("image_frame", ImageFrame)
    def on_frame(data):
        logger.info(f"{time.time()} on_frame -> frame_id={data.frame_id}")
        if isinstance(data, dict):
            data["processed"] = True
            app.publish("image_frame_processed", data)
        else:
            app.publish("image_frame_processed", {"id": data.id, "frame_id": data.frame_id, "processed": True})

    listener_thread = threading.Thread(target=app._listen_loop, daemon=True)
    listener_thread.start()

    time.sleep(1)
    open_result = app.get(
        "stream_open",
        {"id": "stream-001", "rtmp_url": "rtmp://127.0.0.1:1935/live/test_stream", "rtmp_result_url": "rtmp://127.0.0.1:1935/live/test_stream_out"},
        timeout=5.0,
    )
    print(f"stream_open -> {open_result}")

    time.sleep(10)
    close_result = app.get("stream_close", {"id": "stream-001"}, timeout=5.0)
    print(f"stream_close -> {close_result}")

    time.sleep(3)
    app.stop()


if __name__ == "__main__":
    run_client()
