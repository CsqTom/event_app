import os
import tempfile
import pickle
import uuid
import time
import asyncio
import multiprocessing
import queue
import threading
from typing import Any, Callable, Optional, Dict, List

import zmq
import zmq.asyncio as async_zmq
from pydantic import BaseModel, ValidationError

def _ipc_endpoint(name: str) -> str:
    path = os.path.join(tempfile.gettempdir(), name)
    return f"ipc://{path.replace(os.sep, '/')}"


def _default_zmq_config() -> Dict[str, str]:
    if os.name == "nt":
        return {
            "event_endpoint": "tcp://127.0.0.1:5555",
            "rpc_endpoint": "tcp://127.0.0.1:5556"
        }
    return {
        "event_endpoint": _ipc_endpoint("event_app_event.sock"),
        "rpc_endpoint": _ipc_endpoint("event_app_rpc.sock")
    }


DEFAULT_ZMQ_CONFIG = _default_zmq_config()


def _cleanup_ipc(endpoint: str):
    if not endpoint.startswith("ipc://"):
        return
    path = endpoint[6:]
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def serialize_data(data: Any) -> bytes:
    return pickle.dumps(data)


def deserialize_data(data_bytes: bytes) -> Any:
    return pickle.loads(data_bytes)


class EventSchemaError(ValueError):
    pass


class EventApp:
    def __new__(cls, redis_config: dict = None, group_name: str = "event_app_group"):
        if os.name == "nt" and redis_config is None:
            from event_app_redis import EventApp as RedisEventApp

            return RedisEventApp(redis_config=None, group_name=group_name)
        return super().__new__(cls)

    def __init__(self, redis_config: dict = None, group_name: str = "event_app_group"):
        self.zmq_config = redis_config or DEFAULT_ZMQ_CONFIG
        self.subscribers: Dict[str, List[Callable]] = {}
        self.rpc_handlers: Dict[str, Callable] = {}
        self.group_name = group_name
        self._publish_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._publish_thread: Optional[threading.Thread] = None
        self._handler_local = threading.local()
        self._event_specs: Dict[str, Dict[str, Any]] = {}
        self._rpc_specs: Dict[str, Dict[str, Any]] = {}

        self._context: Optional[zmq.Context] = None
        self._event_push: Optional[zmq.Socket] = None
        self._event_pull: Optional[zmq.Socket] = None
        self._rpc_rep: Optional[zmq.Socket] = None
        self._rpc_req: Optional[zmq.Socket] = None

        self._async_context: Optional[async_zmq.Context] = None
        self._async_event_push: Optional[async_zmq.Socket] = None
        self._async_event_pull: Optional[async_zmq.Socket] = None
        self._async_rpc_rep: Optional[async_zmq.Socket] = None
        self._async_rpc_req: Optional[async_zmq.Socket] = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_context"] = None
        state["_event_push"] = None
        state["_event_pull"] = None
        state["_rpc_rep"] = None
        state["_rpc_req"] = None
        state["_async_context"] = None
        state["_async_event_push"] = None
        state["_async_event_pull"] = None
        state["_async_rpc_rep"] = None
        state["_async_rpc_req"] = None
        state["_publish_queue"] = None
        state["_publish_thread"] = None
        state["_handler_local"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self.__dict__.get("_publish_queue") is None:
            self._publish_queue = queue.Queue(maxsize=1000)
        self._publish_thread = None
        if self.__dict__.get("_handler_local") is None:
            self._handler_local = threading.local()

    def _in_handler(self) -> bool:
        return getattr(self._handler_local, "active", False)

    def _ensure_model(self, model: type[BaseModel], label: str) -> type[BaseModel]:
        if model is None:
            raise EventSchemaError(f"{label} model required")
        try:
            if not issubclass(model, BaseModel):
                raise EventSchemaError(f"{label} model must inherit BaseModel")
        except TypeError as exc:
            raise EventSchemaError(f"{label} model must inherit BaseModel") from exc
        return model

    def _normalize_payload(self, data: Any) -> Any:
        if isinstance(data, BaseModel):
            return data.model_dump()
        return data

    def _coerce_model(self, model: type[BaseModel], data: Any) -> Any:
        if model is None:
            return data
        if isinstance(data, model):
            return data
        if isinstance(data, BaseModel):
            data = data.model_dump()
        return model.model_validate(data)

    def _validate_model(self, model: type[BaseModel], data: Any) -> None:
        if model is None:
            return
        try:
            self._coerce_model(model, data)
        except ValidationError as exc:
            raise EventSchemaError(str(exc)) from exc

    def _validate_event_payload(self, event_type: str, data: Any) -> None:
        spec = self._event_specs.get(event_type)
        if not spec:
            return
        model = spec.get("model")
        self._validate_model(model, data)

    def _coerce_event_payload(self, event_type: str, data: Any) -> Any:
        spec = self._event_specs.get(event_type)
        if not spec:
            return data
        model = spec.get("model")
        return self._coerce_model(model, data)

    def _validate_rpc_request(self, event_type: str, data: Any) -> None:
        spec = self._rpc_specs.get(event_type)
        if not spec:
            return
        model = spec.get("request_model")
        self._validate_model(model, data)

    def _coerce_rpc_request(self, event_type: str, data: Any) -> Any:
        spec = self._rpc_specs.get(event_type)
        if not spec:
            return data
        model = spec.get("request_model")
        return self._coerce_model(model, data)

    def _validate_rpc_response(self, event_type: str, data: Any) -> None:
        spec = self._rpc_specs.get(event_type)
        if not spec:
            return
        model = spec.get("response_model")
        self._validate_model(model, data)

    def _event_endpoint(self) -> str:
        return self.zmq_config.get("event_endpoint", DEFAULT_ZMQ_CONFIG["event_endpoint"])

    def _rpc_endpoint(self) -> str:
        return self.zmq_config.get("rpc_endpoint", DEFAULT_ZMQ_CONFIG["rpc_endpoint"])

    def _init_context(self):
        if self._context is None:
            self._context = zmq.Context.instance()

    def _init_async_context(self):
        if self._async_context is None:
            self._async_context = async_zmq.Context.instance()

    def _get_event_push(self) -> zmq.Socket:
        self._init_context()
        if self._event_push is None:
            self._event_push = self._context.socket(zmq.PUSH)
            self._event_push.linger = 0
            self._event_push.connect(self._event_endpoint())
        return self._event_push

    def _get_async_event_push(self) -> async_zmq.Socket:
        self._init_async_context()
        if self._async_event_push is None:
            self._async_event_push = self._async_context.socket(zmq.PUSH)
            self._async_event_push.linger = 0
            self._async_event_push.connect(self._event_endpoint())
        return self._async_event_push

    def _get_rpc_req(self) -> zmq.Socket:
        self._init_context()
        if self._rpc_req is None:
            self._rpc_req = self._context.socket(zmq.REQ)
            self._rpc_req.linger = 0
            self._rpc_req.connect(self._rpc_endpoint())
        return self._rpc_req

    def _get_async_rpc_req(self) -> async_zmq.Socket:
        self._init_async_context()
        if self._async_rpc_req is None:
            self._async_rpc_req = self._async_context.socket(zmq.REQ)
            self._async_rpc_req.linger = 0
            self._async_rpc_req.connect(self._rpc_endpoint())
        return self._async_rpc_req

    def subscribe(self, event_type: str, model: type[BaseModel]):
        model = self._ensure_model(model, "event")
        existing = self._event_specs.get(event_type)
        if existing and existing.get("model") is not model:
            raise EventSchemaError(f"event model already registered for {event_type}")
        self._event_specs[event_type] = {"model": model}

        def decorator(func: Callable) -> Callable:
            if event_type not in self.subscribers:
                self.subscribers[event_type] = []
            self.subscribers[event_type].append(func)
            return func

        return decorator

    def rpc(self, event_type: str, request_model: type[BaseModel], response_model: type[BaseModel]):
        request_model = self._ensure_model(request_model, "request")
        response_model = self._ensure_model(response_model, "response")
        existing = self._rpc_specs.get(event_type)
        if existing:
            if existing.get("request_model") is not request_model or existing.get("response_model") is not response_model:
                raise EventSchemaError(f"rpc model already registered for {event_type}")
        self._rpc_specs[event_type] = {"request_model": request_model, "response_model": response_model}

        def decorator(func: Callable) -> Callable:
            self.rpc_handlers[event_type] = func
            return func

        return decorator

    def publish(self, event_type: str, data: Any) -> None:
        self._validate_event_payload(event_type, data)
        data = self._normalize_payload(data)
        if self._in_handler():
            self._enqueue_publish(event_type, data)
            return
        publish_data = {
            "event_type": event_type,
            "data": data,
            "need_response": False,
            "request_id": None
        }
        socket = self._get_event_push()
        socket.send(serialize_data(publish_data))

    def _publish_worker(self):
        while True:
            event_type, data = self._publish_queue.get()
            try:
                self.publish(event_type, data)
            finally:
                self._publish_queue.task_done()

    def _ensure_publish_worker(self):
        if self._publish_thread is None or not self._publish_thread.is_alive():
            self._publish_thread = threading.Thread(target=self._publish_worker, daemon=True)
            self._publish_thread.start()

    def _enqueue_publish(self, event_type: str, data: Any) -> None:
        self._ensure_publish_worker()
        try:
            self._publish_queue.put_nowait((event_type, data))
        except queue.Full:
            pass

    def _publish_from_handler(self, result: Any) -> None:
        if result is None:
            return
        if isinstance(result, tuple) and len(result) == 2:
            self._validate_event_payload(result[0], result[1])
            self._enqueue_publish(result[0], self._normalize_payload(result[1]))
            return
        if isinstance(result, dict) and "event_type" in result and "data" in result:
            self._validate_event_payload(result["event_type"], result["data"])
            self._enqueue_publish(result["event_type"], self._normalize_payload(result["data"]))
            return
        if isinstance(result, (list, tuple)):
            for item in result:
                self._publish_from_handler(item)

    async def _publish_from_handler_async(self, result: Any) -> None:
        if result is None:
            return
        if isinstance(result, tuple) and len(result) == 2:
            self._validate_event_payload(result[0], result[1])
            self._enqueue_publish(result[0], self._normalize_payload(result[1]))
            return
        if isinstance(result, dict) and "event_type" in result and "data" in result:
            self._validate_event_payload(result["event_type"], result["data"])
            self._enqueue_publish(result["event_type"], self._normalize_payload(result["data"]))
            return
        if isinstance(result, (list, tuple)):
            for item in result:
                await self._publish_from_handler_async(item)

    def get(self, event_type: str, data: Any, timeout: float = 10.0) -> Any:
        self._validate_rpc_request(event_type, data)
        data = self._normalize_payload(data)
        request_id = str(uuid.uuid4())
        publish_data = {
            "event_type": event_type,
            "data": data,
            "need_response": True,
            "request_id": request_id,
            "timestamp": time.time(),
            "timeout": timeout
        }
        socket = self._get_rpc_req()
        socket.send(serialize_data(publish_data))
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        timeout_ms = int(max(timeout, 1) * 1000)
        events = dict(poller.poll(timeout_ms))
        if socket not in events:
            raise TimeoutError(f"RPC调用 {event_type} 超时（{timeout}s）")
        payload = socket.recv()
        result_data = deserialize_data(payload)
        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    async def publish_async(self, event_type: str, data: Any) -> None:
        self._validate_event_payload(event_type, data)
        data = self._normalize_payload(data)
        publish_data = {
            "event_type": event_type,
            "data": data,
            "need_response": False,
            "request_id": None
        }
        socket = self._get_async_event_push()
        await socket.send(serialize_data(publish_data))

    async def get_async(self, event_type: str, data: Any, timeout: float = 10.0) -> Any:
        self._validate_rpc_request(event_type, data)
        data = self._normalize_payload(data)
        request_id = str(uuid.uuid4())
        publish_data = {
            "event_type": event_type,
            "data": data,
            "need_response": True,
            "request_id": request_id,
            "timestamp": time.time(),
            "timeout": timeout
        }
        socket = self._get_async_rpc_req()
        await socket.send(serialize_data(publish_data))
        poller = async_zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        timeout_ms = int(max(timeout, 1) * 1000)
        events = dict(await poller.poll(timeout_ms))
        if socket not in events:
            raise TimeoutError(f"异步RPC调用 {event_type} 超时（{timeout}s）")
        payload = await socket.recv()
        result_data = deserialize_data(payload)
        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"Async RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    def _process_event(self, event_type: str, publish_data: dict):
        data = publish_data["data"]
        need_response = publish_data.get("need_response", False)
        request_id = publish_data.get("request_id")
        if need_response:
            try:
                self._validate_rpc_request(event_type, data)
            except EventSchemaError as e:
                return {"request_id": request_id, "error": str(e)}
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")
            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    return {"request_id": request_id, "error": "timeout"}
            if event_type in self.rpc_handlers:
                try:
                    handler = self.rpc_handlers[event_type]
                    result = handler(self._coerce_rpc_request(event_type, data))
                    self._validate_rpc_response(event_type, result)
                    return {"request_id": request_id, "data": self._normalize_payload(result)}
                except Exception as e:
                    return {"request_id": request_id, "error": str(e)}
            return {"request_id": request_id, "error": f"no handler for {event_type}"}
        if event_type in self.subscribers:
            try:
                self._validate_event_payload(event_type, data)
            except EventSchemaError:
                return None
            for handler in self.subscribers[event_type]:
                try:
                    self._handler_local.active = True
                    try:
                        result = handler(self._coerce_event_payload(event_type, data))
                    finally:
                        self._handler_local.active = False
                    self._publish_from_handler(result)
                except Exception:
                    pass
        return None

    async def _process_event_async(self, event_type: str, publish_data: dict):
        data = publish_data["data"]
        need_response = publish_data.get("need_response", False)
        request_id = publish_data.get("request_id")
        if need_response:
            try:
                self._validate_rpc_request(event_type, data)
            except EventSchemaError as e:
                return {"request_id": request_id, "error": str(e)}
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")
            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    return {"request_id": request_id, "error": "timeout"}
            if event_type in self.rpc_handlers:
                try:
                    handler = self.rpc_handlers[event_type]
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(self._coerce_rpc_request(event_type, data))
                    else:
                        result = handler(self._coerce_rpc_request(event_type, data))
                    self._validate_rpc_response(event_type, result)
                    return {"request_id": request_id, "data": self._normalize_payload(result)}
                except Exception as e:
                    return {"request_id": request_id, "error": str(e)}
            return {"request_id": request_id, "error": f"no handler for {event_type}"}
        if event_type in self.subscribers:
            try:
                self._validate_event_payload(event_type, data)
            except EventSchemaError:
                return None
            for handler in self.subscribers[event_type]:
                try:
                    self._handler_local.active = True
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            result = await handler(self._coerce_event_payload(event_type, data))
                        else:
                            result = handler(self._coerce_event_payload(event_type, data))
                    finally:
                        self._handler_local.active = False
                    await self._publish_from_handler_async(result)
                except Exception:
                    pass
        return None

    def _listen_loop(self):
        self._init_context()
        _cleanup_ipc(self._event_endpoint())
        _cleanup_ipc(self._rpc_endpoint())
        if self._event_pull is None:
            self._event_pull = self._context.socket(zmq.PULL)
            self._event_pull.linger = 0
            self._event_pull.bind(self._event_endpoint())
        if self._rpc_rep is None:
            self._rpc_rep = self._context.socket(zmq.REP)
            self._rpc_rep.linger = 0
            self._rpc_rep.bind(self._rpc_endpoint())
        print(f"事件监听已启动 (PID: {multiprocessing.current_process().pid})，监听事件: {list(self.subscribers.keys())}")
        poller = zmq.Poller()
        poller.register(self._event_pull, zmq.POLLIN)
        poller.register(self._rpc_rep, zmq.POLLIN)
        while True:
            events = dict(poller.poll(1000))
            if self._event_pull in events:
                payload = self._event_pull.recv()
                publish_data = deserialize_data(payload)
                event_type = publish_data.get("event_type")
                if event_type:
                    self._process_event(event_type, publish_data)
            if self._rpc_rep in events:
                payload = self._rpc_rep.recv()
                publish_data = deserialize_data(payload)
                event_type = publish_data.get("event_type")
                response = {"request_id": publish_data.get("request_id"), "error": "invalid request"}
                if event_type:
                    response = self._process_event(event_type, publish_data) or response
                self._rpc_rep.send(serialize_data(response))

    def run(self, block: bool = True):
        if block:
            self._listen_loop()
        else:
            listener_thread = multiprocessing.Process(target=self._listen_loop)
            listener_thread.daemon = True
            listener_thread.start()

    async def run_async(self):
        self._init_async_context()
        _cleanup_ipc(self._event_endpoint())
        _cleanup_ipc(self._rpc_endpoint())
        if self._async_event_pull is None:
            self._async_event_pull = self._async_context.socket(zmq.PULL)
            self._async_event_pull.linger = 0
            self._async_event_pull.bind(self._event_endpoint())
        if self._async_rpc_rep is None:
            self._async_rpc_rep = self._async_context.socket(zmq.REP)
            self._async_rpc_rep.linger = 0
            self._async_rpc_rep.bind(self._rpc_endpoint())
        print(f"异步事件监听已启动，监听事件: {list(self.subscribers.keys())}")
        poller = async_zmq.Poller()
        poller.register(self._async_event_pull, zmq.POLLIN)
        poller.register(self._async_rpc_rep, zmq.POLLIN)
        while True:
            events = dict(await poller.poll(1000))
            if self._async_event_pull in events:
                payload = await self._async_event_pull.recv()
                publish_data = deserialize_data(payload)
                event_type = publish_data.get("event_type")
                if event_type:
                    await self._process_event_async(event_type, publish_data)
            if self._async_rpc_rep in events:
                payload = await self._async_rpc_rep.recv()
                publish_data = deserialize_data(payload)
                event_type = publish_data.get("event_type")
                response = {"request_id": publish_data.get("request_id"), "error": "invalid request"}
                if event_type:
                    response = await self._process_event_async(event_type, publish_data) or response
                await self._async_rpc_rep.send(serialize_data(response))
