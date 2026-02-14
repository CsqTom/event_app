import os
import time
import threading
from typing import Dict, Any, Optional

from pydantic import BaseModel

from event_app_redis import EventApp


def load_image_bytes() -> bytes:
    file_path = os.path.join(os.path.dirname(__file__), "260203090900688_frame2225.jpg")
    with open(file_path, "rb") as f:
        return f.read()


class StreamTaskManager:
    def __init__(self, app: EventApp, payload: bytes):
        self.app = app
        self.payload = payload
        self._stream_threads: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}
        self._frame_ids: Dict[str, int] = {}

    def open(self, data: Dict[str, Any]):
        stream_id = data.get("id")
        if not stream_id:
            return {"status": "error", "error": "missing id"}
        if stream_id in self._stream_threads:
            return {"status": "opened", "id": stream_id}
        stop_flag = threading.Event()
        self._stop_flags[stream_id] = stop_flag
        self._frame_ids.setdefault(stream_id, 0)
        thread = threading.Thread(target=self._publish_loop, args=(stream_id, stop_flag), daemon=True)
        self._stream_threads[stream_id] = thread
        thread.start()
        return {"status": "opened", "id": stream_id}

    def close(self, data: Dict[str, Any]):
        stream_id = data.get("id")
        if not stream_id:
            return {"status": "error", "error": "missing id"}
        stop_flag = self._stop_flags.pop(stream_id, None)
        if stop_flag is not None:
            stop_flag.set()
        self._stream_threads.pop(stream_id, None)
        self._frame_ids.pop(stream_id, None)
        return {"status": "closed", "id": stream_id}

    def _publish_loop(self, stream_id: str, stop_flag: threading.Event):
        while not stop_flag.is_set():
            frame_id = self._frame_ids.get(stream_id, 0)
            self.app.publish(
                "image_frame",
                {"id": stream_id, "frame_id": frame_id, "data": self.payload},
            )
            self._frame_ids[stream_id] = frame_id + 1
            time.sleep(0.1)


def run_server():
    app = EventApp(group_name="video_stream_server")
    payload = load_image_bytes()
    manager = StreamTaskManager(app, payload)

    class StreamOpenRequest(BaseModel):
        id: str
        rtmp_url: str
        rtmp_result_url: str

    class StreamCloseRequest(BaseModel):
        id: str

    class StreamResponse(BaseModel):
        status: str
        id: Optional[str] = None
        error: Optional[str] = None

    @app.rpc("stream_open", StreamOpenRequest, StreamResponse)
    def stream_open(data: Dict[str, Any]):
        return manager.open(data)

    @app.rpc("stream_close", StreamCloseRequest, StreamResponse)
    def stream_close(data: Dict[str, Any]):
        return manager.close(data)

    app.run(block=True)


if __name__ == "__main__":
    run_server()
