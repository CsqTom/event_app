import pickle
import uuid
import time
import asyncio
import multiprocessing
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Dict, List, Union
import numpy as np

import redis
import redis.asyncio as async_redis
from pydantic import BaseModel, ValidationError

try:
    import msgpack
    import msgpack_numpy as m
    m.patch()
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

DEFAULT_REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6380,
    "db": 0,
    "decode_responses": False
}

SERIALIZATION_MSGPACK = "msgpack"
SERIALIZATION_PICKLE = "pickle"
DEFAULT_SERIALIZATION = SERIALIZATION_MSGPACK if HAS_MSGPACK else SERIALIZATION_PICKLE


def serialize_data(data: Any, method: str = DEFAULT_SERIALIZATION) -> bytes:
    if method == SERIALIZATION_MSGPACK and HAS_MSGPACK:
        return msgpack.packb(data, use_bin_type=True)
    return pickle.dumps(data)


def deserialize_data(data_bytes: bytes, method: str = DEFAULT_SERIALIZATION) -> Any:
    if method == SERIALIZATION_MSGPACK and HAS_MSGPACK:
        return msgpack.unpackb(data_bytes, raw=False)
    return pickle.loads(data_bytes)


class EventSchemaError(ValueError):
    pass


class SyncRedisClient:
    _instance: Optional[redis.Redis] = None

    @classmethod
    def get_instance(cls, redis_config: dict = None) -> redis.Redis:
        if cls._instance is None or not cls._instance.ping():
            cls._instance = redis.Redis(**(redis_config or DEFAULT_REDIS_CONFIG))
        return cls._instance


class AsyncRedisClient:
    _instance: Optional[async_redis.Redis] = None

    @classmethod
    async def get_instance(cls, redis_config: dict = None) -> async_redis.Redis:
        if cls._instance is None:
            cls._instance = async_redis.Redis(**(redis_config or DEFAULT_REDIS_CONFIG))
        return cls._instance


class EventApp:
    def __init__(
        self,
        redis_config: dict = None,
        group_name: str = "event_app_group",
        serialization: str = DEFAULT_SERIALIZATION,
        max_workers: int = 4,
        batch_size: int = 10,
        block_timeout_ms: int = 50,
    ):
        self.redis_config = redis_config or DEFAULT_REDIS_CONFIG
        self.serialization = serialization
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.block_timeout_ms = block_timeout_ms

        self.subscribers: Dict[str, List[Callable]] = {}
        self.rpc_handlers: Dict[str, Callable] = {}

        self.sync_redis: Optional[redis.Redis] = None
        self.async_redis: Optional[async_redis.Redis] = None
        self.consumer_group = group_name
        self._publish_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._publish_thread: Optional[threading.Thread] = None
        self._handler_local = threading.local()
        self._event_specs: Dict[str, Dict[str, Any]] = {}
        self._rpc_specs: Dict[str, Dict[str, Any]] = {}

        self._executor: Optional[ThreadPoolExecutor] = None
        self._pubsub_thread: Optional[threading.Thread] = None
        self._stream_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self._running.set()

    def _init_sync_redis(self):
        if self.sync_redis is None:
            self.sync_redis = SyncRedisClient.get_instance(self.redis_config)

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor

    def __getstate__(self):
        state = self.__dict__.copy()
        state['sync_redis'] = None
        state['async_redis'] = None
        state['_publish_queue'] = None
        state['_publish_thread'] = None
        state['_handler_local'] = None
        state['_executor'] = None
        state['_pubsub_thread'] = None
        state['_stream_thread'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self.__dict__.get("_publish_queue") is None:
            self._publish_queue = queue.Queue(maxsize=1000)
        self._publish_thread = None
        if self.__dict__.get("_handler_local") is None:
            self._handler_local = threading.local()
        self._running = threading.Event()
        self._running.set()

    def _in_handler(self) -> bool:
        return getattr(self._handler_local, "active", False)

    def _stream_key(self, event_type: str) -> str:
        return f"event:{event_type}:stream"

    def _channel_key(self, event_type: str) -> str:
        return f"event:{event_type}:channel"

    def _event_type_from_channel(self, channel: Union[str, bytes]) -> str:
        if isinstance(channel, bytes):
            channel = channel.decode()
        prefix = "event:"
        suffix = ":channel"
        if channel.startswith(prefix) and channel.endswith(suffix):
            return channel[len(prefix):-len(suffix)]
        return channel

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

    def _ensure_group(self, stream_key: str, start_id: str = "0", reset_last_id: bool = False):
        try:
            self.sync_redis.xgroup_create(stream_key, self.consumer_group, id=start_id, mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            if reset_last_id:
                self.sync_redis.xgroup_setid(stream_key, self.consumer_group, start_id)

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
        self._init_sync_redis()

        publish_data = {
            "data": data,
            "need_response": False,
            "request_id": None
        }

        channel_key = self._channel_key(event_type)
        self.sync_redis.publish(channel_key, serialize_data(publish_data, self.serialization))

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
        self._init_sync_redis()
        self._validate_rpc_request(event_type, data)
        data = self._normalize_payload(data)

        request_id = str(uuid.uuid4())
        response_key = f"event:response:{request_id}"

        publish_data = {
            "data": data,
            "need_response": True,
            "request_id": request_id,
            "reply_to": response_key,
            "timestamp": time.time(),
            "timeout": timeout
        }

        stream_key = self._stream_key(event_type)

        self.sync_redis.xadd(stream_key, {"payload": serialize_data(publish_data, self.serialization)})

        blpop_timeout = int(timeout) if timeout >= 1 else 1

        try:
            result_tuple = self.sync_redis.blpop(response_key, timeout=blpop_timeout)
        except redis.exceptions.TimeoutError:
            result_tuple = None

        if not result_tuple:
            self.sync_redis.delete(response_key)
            raise TimeoutError(f"RPC调用 {event_type} 超时（{timeout}s）")

        _, payload = result_tuple

        self.sync_redis.delete(response_key)

        result_data = deserialize_data(payload, self.serialization)

        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    async def publish_async(self, event_type: str, data: Any) -> None:
        self._validate_event_payload(event_type, data)
        if self._in_handler():
            self._enqueue_publish(event_type, data)
            return
        data = self._normalize_payload(data)
        if self.async_redis is None:
            self.async_redis = await AsyncRedisClient.get_instance(self.redis_config)

        publish_data = {
            "data": data,
            "need_response": False,
            "request_id": None
        }

        channel_key = self._channel_key(event_type)
        await self.async_redis.publish(channel_key, serialize_data(publish_data, self.serialization))

    async def get_async(self, event_type: str, data: Any, timeout: float = 10.0) -> Any:
        if self.async_redis is None:
            self.async_redis = await AsyncRedisClient.get_instance(self.redis_config)
        self._validate_rpc_request(event_type, data)
        data = self._normalize_payload(data)

        request_id = str(uuid.uuid4())
        response_key = f"event:response:{request_id}"

        publish_data = {
            "data": data,
            "need_response": True,
            "request_id": request_id,
            "reply_to": response_key,
            "timestamp": time.time(),
            "timeout": timeout
        }

        stream_key = self._stream_key(event_type)

        await self.async_redis.xadd(stream_key, {"payload": serialize_data(publish_data, self.serialization)})

        blpop_timeout = int(timeout) if timeout >= 1 else 1
        try:
            result_tuple = await self.async_redis.blpop(response_key, timeout=blpop_timeout)
        except redis.exceptions.TimeoutError:
            result_tuple = None

        if not result_tuple:
            await self.async_redis.delete(response_key)
            raise TimeoutError(f"异步RPC调用 {event_type} 超时（{timeout}s）")

        _, payload = result_tuple

        await self.async_redis.delete(response_key)

        result_data = deserialize_data(payload, self.serialization)

        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"Async RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    def _process_event(self, event_type: str, publish_data: dict):
        data = publish_data["data"]
        need_response = publish_data.get("need_response", False)
        request_id = publish_data.get("request_id")
        reply_to = publish_data.get("reply_to")

        if need_response:
            try:
                self._validate_rpc_request(event_type, data)
            except EventSchemaError as e:
                if reply_to:
                    self.sync_redis.rpush(
                        reply_to,
                        serialize_data({"request_id": request_id, "error": str(e)}, self.serialization),
                    )
                    self.sync_redis.expire(reply_to, 60)
                return
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")

            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    print(f"RPC请求 {event_type} (ID: {request_id}) 已超时，丢弃处理")
                    return

            if event_type in self.rpc_handlers:
                try:
                    handler = self.rpc_handlers[event_type]
                    self._handler_local.active = True
                    try:
                        result = handler(self._coerce_rpc_request(event_type, data))
                    finally:
                        self._handler_local.active = False
                    self._validate_rpc_response(event_type, result)
                    if reply_to:
                        normalized = self._normalize_payload(result)
                        self.sync_redis.rpush(reply_to, serialize_data({"request_id": request_id, "data": normalized}, self.serialization))
                        self.sync_redis.expire(reply_to, 60)
                except Exception as e:
                    print(f"RPC {event_type} 处理失败: {e}")
                    if reply_to:
                        self.sync_redis.rpush(reply_to, serialize_data({"request_id": request_id, "error": str(e)}, self.serialization))
                        self.sync_redis.expire(reply_to, 60)
            else:
                print(f"收到RPC请求 {event_type} 但无处理器")
        else:
            if event_type in self.subscribers:
                try:
                    self._validate_event_payload(event_type, data)
                except EventSchemaError as e:
                    print(f"事件 {event_type} payload 校验失败: {e}")
                    return
                for handler in self.subscribers[event_type]:
                    try:
                        self._handler_local.active = True
                        try:
                            result = handler(self._coerce_event_payload(event_type, data))
                        finally:
                            self._handler_local.active = False
                        self._publish_from_handler(result)
                    except Exception as e:
                        print(f"订阅处理器 {handler.__name__} 处理 {event_type} 失败: {e}")
            else:
                if event_type not in self.rpc_handlers:
                    pass

    def _process_event_async_wrapper(self, event_type: str, publish_data: dict):
        try:
            self._process_event(event_type, publish_data)
        except Exception as e:
            print(f"事件处理异常: {e}")

    async def _process_event_async(self, event_type: str, publish_data: dict):
        data = publish_data["data"]
        need_response = publish_data.get("need_response", False)
        request_id = publish_data.get("request_id")
        reply_to = publish_data.get("reply_to")

        if need_response:
            try:
                self._validate_rpc_request(event_type, data)
            except EventSchemaError as e:
                if reply_to:
                    await self.async_redis.rpush(
                        reply_to,
                        serialize_data({"request_id": request_id, "error": str(e)}, self.serialization),
                    )
                    await self.async_redis.expire(reply_to, 60)
                return
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")

            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    print(f"Async RPC请求 {event_type} (ID: {request_id}) 已超时，丢弃处理")
                    return

            if event_type in self.rpc_handlers:
                try:
                    handler = self.rpc_handlers[event_type]
                    self._handler_local.active = True
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            result = await handler(self._coerce_rpc_request(event_type, data))
                        else:
                            result = handler(self._coerce_rpc_request(event_type, data))
                    finally:
                        self._handler_local.active = False
                    self._validate_rpc_response(event_type, result)
                    if reply_to:
                        normalized = self._normalize_payload(result)
                        await self.async_redis.rpush(reply_to,
                                                     serialize_data({"request_id": request_id, "data": normalized}, self.serialization))
                        await self.async_redis.expire(reply_to, 60)
                except Exception as e:
                    print(f"Async RPC {event_type} 处理失败: {e}")
                    if reply_to:
                        await self.async_redis.rpush(reply_to,
                                                     serialize_data({"request_id": request_id, "error": str(e)}, self.serialization))
                        await self.async_redis.expire(reply_to, 60)
        else:
            if event_type in self.subscribers:
                try:
                    self._validate_event_payload(event_type, data)
                except EventSchemaError as e:
                    print(f"Async 事件 {event_type} payload 校验失败: {e}")
                    return
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
                    except Exception as e:
                        print(f"Async 订阅处理器 {handler.__name__} 处理 {event_type} 失败: {e}")

    def _pubsub_listen_loop(self):
        if self.sync_redis is None:
            self.sync_redis = SyncRedisClient.get_instance(self.redis_config)

        channel_keys = [self._channel_key(event_type) for event_type in self.subscribers.keys()]
        if not channel_keys:
            return

        pubsub = self.sync_redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(*channel_keys)

        print(f"[PubSub] 监听已启动，订阅: {list(self.subscribers.keys())}")

        executor = self._get_executor()
        futures = []

        while self._running.is_set():
            try:
                message = pubsub.get_message(timeout=0.1)
                if message and message.get("type") == "message":
                    channel = message.get("channel")
                    payload = message.get("data")
                    event_type = self._event_type_from_channel(channel)
                    if payload:
                        try:
                            publish_data = deserialize_data(payload, self.serialization)
                            future = executor.submit(self._process_event_async_wrapper, event_type, publish_data)
                            futures.append(future)
                        except Exception as e:
                            print(f"PubSub Data process error: {e}")

                futures = [f for f in futures if not f.done()]
            except Exception as e:
                print(f"PubSub loop error: {e}")

        pubsub.unsubscribe()
        pubsub.close()

    def _stream_listen_loop(self):
        if self.sync_redis is None:
            self.sync_redis = SyncRedisClient.get_instance(self.redis_config)

        all_events = set(self.rpc_handlers.keys())
        stream_keys = []
        for event_type in all_events:
            stream_key = self._stream_key(event_type)
            stream_keys.append(stream_key)
            self._ensure_group(stream_key, start_id="$", reset_last_id=True)

        if not stream_keys:
            return

        print(f"[Stream] 监听已启动，RPC: {list(all_events)}")

        consumer_name = f"consumer-{multiprocessing.current_process().pid}"
        executor = self._get_executor()
        futures = []

        while self._running.is_set():
            streams = {key: ">" for key in stream_keys}
            try:
                messages = self.sync_redis.xreadgroup(
                    self.consumer_group,
                    consumer_name,
                    streams,
                    count=self.batch_size,
                    block=self.block_timeout_ms
                )
            except redis.exceptions.ResponseError as e:
                print(f"Redis Read Error: {e}")
                time.sleep(1)
                continue

            if not messages:
                continue

            for stream_key, entries in messages:
                stream_name = stream_key.decode() if isinstance(stream_key, bytes) else stream_key
                if stream_name.startswith("event:") and stream_name.endswith(":stream"):
                    event_type = stream_name[6:-7]
                else:
                    event_type = stream_name

                for message_id, fields in entries:
                    payload = fields.get(b"payload") or fields.get("payload")
                    if not payload:
                        self.sync_redis.xack(stream_name, self.consumer_group, message_id)
                        continue

                    try:
                        publish_data = deserialize_data(payload, self.serialization)
                        future = executor.submit(self._process_event_async_wrapper, event_type, publish_data)
                        futures.append(future)
                    except Exception as e:
                        print(f"Data process error: {e}")

                    self.sync_redis.xack(stream_name, self.consumer_group, message_id)
                    self.sync_redis.xdel(stream_name, message_id)

            futures = [f for f in futures if not f.done()]

    def _listen_loop(self):
        self._init_sync_redis()

        has_pubsub = bool(self.subscribers)
        has_stream = bool(self.rpc_handlers)

        if has_pubsub and has_stream:
            self._pubsub_thread = threading.Thread(target=self._pubsub_listen_loop, daemon=True)
            self._pubsub_thread.start()

            self._stream_listen_loop()
        elif has_pubsub:
            self._pubsub_listen_loop()
        elif has_stream:
            self._stream_listen_loop()
        else:
            print("无订阅者或RPC处理器，监听未启动")

    def run(self, block: bool = True):
        if block:
            self._listen_loop()
        else:
            listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
            listener_thread.start()

    def stop(self):
        self._running.clear()
        if self._executor:
            self._executor.shutdown(wait=False)

    async def run_async(self):
        if self.async_redis is None:
            self.async_redis = await AsyncRedisClient.get_instance(self.redis_config)

        channel_keys = [self._channel_key(event_type) for event_type in self.subscribers.keys()]
        pubsub = None
        if channel_keys:
            pubsub = self.async_redis.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(*channel_keys)

        async def pubsub_loop():
            if pubsub is None:
                return
            async for message in pubsub.listen():
                if not message or message.get("type") != "message":
                    continue
                channel = message.get("channel")
                payload = message.get("data")
                event_type = self._event_type_from_channel(channel)
                if not payload:
                    continue
                try:
                    publish_data = deserialize_data(payload, self.serialization)
                    await self._process_event_async(event_type, publish_data)
                except Exception as e:
                    print(f"Async PubSub Data process error: {e}")

        asyncio.create_task(pubsub_loop())

        all_events = set(self.rpc_handlers.keys())
        stream_keys = []

        for event_type in all_events:
            stream_key = self._stream_key(event_type)
            stream_keys.append(stream_key)
            try:
                await self.async_redis.xgroup_create(stream_key, self.consumer_group, id="$", mkstream=True)
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
                await self.async_redis.xgroup_setid(stream_key, self.consumer_group, "$")

        print(f"异步事件监听已启动，订阅: {list(self.subscribers.keys())}，RPC: {list(all_events)}")

        consumer_name = f"consumer-{multiprocessing.current_process().pid}"
        while True:
            if not stream_keys:
                await asyncio.sleep(0.05)
                continue

            streams = {key: ">" for key in stream_keys}
            try:
                messages = await self.async_redis.xreadgroup(
                    self.consumer_group,
                    consumer_name,
                    streams,
                    count=self.batch_size,
                    block=self.block_timeout_ms
                )
            except redis.exceptions.ResponseError as e:
                print(f"Async Redis Read Error: {e}")
                await asyncio.sleep(1)
                continue

            for stream_key, entries in messages:
                stream_name = stream_key.decode() if isinstance(stream_key, bytes) else stream_key
                if stream_name.startswith("event:") and stream_name.endswith(":stream"):
                    event_type = stream_name[6:-7]
                else:
                    event_type = stream_name

                for message_id, fields in entries:
                    payload = fields.get(b"payload") or fields.get("payload")
                    if not payload:
                        await self.async_redis.xack(stream_name, self.consumer_group, message_id)
                        continue

                    try:
                        publish_data = deserialize_data(payload, self.serialization)
                        await self._process_event_async(event_type, publish_data)
                    except Exception as e:
                        print(f"Async Data process error: {e}")

                    await self.async_redis.xack(stream_name, self.consumer_group, message_id)
                    await self.async_redis.xdel(stream_name, message_id)
