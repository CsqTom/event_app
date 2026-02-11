import os
import time
import multiprocessing
from typing import Any, Dict, Optional

from event_app_redis import EventApp as RedisEventApp
from event_app_zeromq import EventApp as ZmqEventApp


def load_image_bytes() -> bytes:
    file_path = os.path.join(os.path.dirname(__file__), "260203090900688_frame2225.jpg")
    with open(file_path, "rb") as f:
        return f.read()


def run_server(app_cls, config: Optional[Dict[str, Any]], group_name: str):
    app = app_cls(redis_config=config, group_name=group_name)
    counter = {"count": 0}

    @app.subscribe("image_event")
    def on_image(data):
        counter["count"] += 1

    @app.rpc("image_rpc")
    def on_rpc(data):
        return len(data)

    @app.rpc("get_metrics")
    def get_metrics(_):
        return {"count": counter["count"]}

    app.run(block=True)


def run_client(app_cls, config: Optional[Dict[str, Any]], group_name: str, payload: bytes, publish_count: int, rpc_count: int):
    app = app_cls(redis_config=config, group_name=group_name)

    start_publish = time.perf_counter()
    for _ in range(publish_count):
        app.publish("image_event", payload)
    publish_duration = time.perf_counter() - start_publish

    time.sleep(0.5)
    metrics = app.get("get_metrics", None, timeout=10.0)
    received = metrics.get("count", 0) if isinstance(metrics, dict) else 0

    start_rpc = time.perf_counter()
    last_size = None
    for _ in range(rpc_count):
        last_size = app.get("image_rpc", payload, timeout=10.0)
    rpc_duration = time.perf_counter() - start_rpc

    return {
        "publish_duration": publish_duration,
        "publish_count": publish_count,
        "received_count": received,
        "rpc_duration": rpc_duration,
        "rpc_count": rpc_count,
        "last_rpc_size": last_size,
    }


def benchmark(name: str, app_cls, config: Optional[Dict[str, Any]], payload: bytes):
    group_name = f"{name}_perf_group"
    publish_count = 20
    rpc_count = 10

    server_process = multiprocessing.Process(target=run_server, args=(app_cls, config, group_name))
    server_process.start()
    time.sleep(1)
    try:
        result = run_client(app_cls, config, group_name, payload, publish_count, rpc_count)
    finally:
        server_process.terminate()
        server_process.join()

    publish_rate = result["publish_count"] / result["publish_duration"] if result["publish_duration"] > 0 else 0
    rpc_rate = result["rpc_count"] / result["rpc_duration"] if result["rpc_duration"] > 0 else 0
    avg_rpc_ms = (result["rpc_duration"] / result["rpc_count"]) * 1000 if result["rpc_count"] > 0 else 0

    print(f"{name} payload_bytes={len(payload)}")
    print(f"{name} publish_duration_s={result['publish_duration']:.4f} publish_rate_msg_s={publish_rate:.2f} publish_received={result['received_count']}")
    print(f"{name} rpc_duration_s={result['rpc_duration']:.4f} rpc_rate_msg_s={rpc_rate:.2f} rpc_avg_ms={avg_rpc_ms:.2f} rpc_last_size={result['last_rpc_size']}")


if __name__ == "__main__":
    payload = load_image_bytes()

    benchmark("redis", RedisEventApp, None, payload)

    benchmark("zeromq", ZmqEventApp, None, payload)
