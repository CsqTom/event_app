import time
import threading

from event_app_redis import EventApp


def run_client():
    app = EventApp(group_name="video_stream_client")
    counters = {"received": 0}

    @app.subscribe("image_frame")
    def on_frame(data):
        counters["received"] += 1
        print(f"on_frame -> {counters['received']}")
        if isinstance(data, dict):
            processed = dict(data)
            processed["processed"] = True
            app.publish("image_frame_processed", processed)

    listener_thread = threading.Thread(target=app._listen_loop, daemon=True)
    listener_thread.start()

    time.sleep(1)
    open_result = app.get(
        "stream_open",
        {"id": "stream-001", "rtmp_url": "rtmp://127.0.0.1:1935/live/test_stream", "rtmp_result_url": "rtmp://127.0.0.1:1935/live/test_stream_out"},
        timeout=5.0,
    )
    print(f"stream_open -> {open_result}")

    time.sleep(2)
    close_result = app.get("stream_close", {"id": "stream-001"}, timeout=5.0)
    print(f"stream_close -> {close_result}")

    time.sleep(0.5)
    print(f"received_frames -> {counters['received']}")


if __name__ == "__main__":
    run_client()
