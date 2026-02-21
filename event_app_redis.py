import pickle
import uuid
import time
import asyncio
import multiprocessing
import queue
import threading
from typing import Any, Callable, Optional, Dict, List, Union
import numpy as np

import redis
import redis.asyncio as async_redis
from pydantic import BaseModel, ValidationError

# -------------------------- 基础配置 & 工具函数 --------------------------
DEFAULT_REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
    "decode_responses": False
}


def serialize_data(data: Any) -> bytes:
    """序列化（支持numpy.ndarray）"""
    return pickle.dumps(data)


def deserialize_data(data_bytes: bytes) -> Any:
    """反序列化"""
    return pickle.loads(data_bytes)


class EventSchemaError(ValueError):
    pass


# -------------------------- Redis 连接管理 --------------------------
class SyncRedisClient:
    """同步Redis单例客户端"""
    _instance: Optional[redis.Redis] = None

    @classmethod
    def get_instance(cls, redis_config: dict = None) -> redis.Redis:
        if cls._instance is None or not cls._instance.ping():
            cls._instance = redis.Redis(**(redis_config or DEFAULT_REDIS_CONFIG))
        return cls._instance


class AsyncRedisClient:
    """异步Redis单例客户端"""
    _instance: Optional[async_redis.Redis] = None

    @classmethod
    async def get_instance(cls, redis_config: dict = None) -> async_redis.Redis:
        if cls._instance is None:
            cls._instance = async_redis.Redis(**(redis_config or DEFAULT_REDIS_CONFIG))
        return cls._instance


# -------------------------- 核心 EventApp 类 --------------------------
class EventApp:
    """
    对于group_name：参见EVENT_REDIS_GROUP_NAME_GUIDE.md说明，或是网上搜 redis consumer_group 相关资料
    """

    def __init__(self, redis_config: dict = None, group_name: str = "event_app_group"):
        self.redis_config = redis_config or DEFAULT_REDIS_CONFIG
        # 订阅者：{ event_type: [func1, func2, ...] }
        self.subscribers: Dict[str, List[Callable]] = {}
        # RPC处理器：{ event_type: func }
        self.rpc_handlers: Dict[str, Callable] = {}

        # Redis 客户端实例
        self.sync_redis: Optional[redis.Redis] = None
        self.async_redis: Optional[async_redis.Redis] = None
        self.consumer_group = group_name
        self._publish_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._publish_thread: Optional[threading.Thread] = None
        self._handler_local = threading.local()
        self._event_specs: Dict[str, Dict[str, Any]] = {}
        self._rpc_specs: Dict[str, Dict[str, Any]] = {}

    def _init_sync_redis(self):
        if self.sync_redis is None:
            # 初始化同步Redis连接, 耗时10-20ms左右
            self.sync_redis = SyncRedisClient.get_instance(self.redis_config)

    def __getstate__(self):
        state = self.__dict__.copy()
        state['sync_redis'] = None
        state['async_redis'] = None
        state['_publish_queue'] = None
        state['_publish_thread'] = None
        state['_handler_local'] = None
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
        """
        订阅装饰器（异步，多订阅）
        支持多个函数订阅同一个事件
        """
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
        """
        RPC装饰器（同步，请求-响应）
        每个事件类型通常只有一个RPC处理器
        """
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
        """
        发布事件（异步/Fire-and-Forget）
        """
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
        self.sync_redis.publish(channel_key, serialize_data(publish_data))

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
        """
        RPC调用（同步/Request-Response）
        """
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

        # 发送请求
        self.sync_redis.xadd(stream_key, {"payload": serialize_data(publish_data)})

        # 阻塞等待响应 (BLPOP)
        # Redis BLPOP timeout must be an integer
        blpop_timeout = int(timeout) if timeout >= 1 else 1

        try:
            # blpop 返回 (key, value) 元组
            result_tuple = self.sync_redis.blpop(response_key, timeout=blpop_timeout)
        except redis.exceptions.TimeoutError:
            # redis-py 的 blpop 在超时时可能抛出异常或返回 None，取决于版本
            result_tuple = None

        if not result_tuple:
            # 清理（虽然可能没数据）
            self.sync_redis.delete(response_key)
            raise TimeoutError(f"RPC调用 {event_type} 超时（{timeout}s）")

        _, payload = result_tuple

        # 清理响应队列
        self.sync_redis.delete(response_key)

        result_data = deserialize_data(payload)

        # 结果结构: {"request_id": ..., "data": ...}
        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    async def publish_async(self, event_type: str, data: Any) -> None:
        """
        异步发布事件
        """
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
        await self.async_redis.publish(channel_key, serialize_data(publish_data))

    async def get_async(self, event_type: str, data: Any, timeout: float = 10.0) -> Any:
        """
        异步RPC调用
        """
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

        await self.async_redis.xadd(stream_key, {"payload": serialize_data(publish_data)})

        # 异步 BLPOP
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

        result_data = deserialize_data(payload)

        if isinstance(result_data, dict) and "error" in result_data:
            raise RuntimeError(f"Async RPC Remote Error: {result_data['error']}")
        result = result_data.get("data")
        self._validate_rpc_response(event_type, result)
        return result

    def _process_event(self, event_type: str, publish_data: dict):
        """处理收到的事件"""
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
                        serialize_data({"request_id": request_id, "error": str(e)}),
                    )
                    self.sync_redis.expire(reply_to, 60)
                return
            # Server-side timeout check
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")

            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    print(f"RPC请求 {event_type} (ID: {request_id}) 已超时，丢弃处理")
                    return  # Skip processing and response

            # RPC 处理
            if event_type in self.rpc_handlers:
                try:
                    handler = self.rpc_handlers[event_type]
                    self._handler_local.active = True
                    try:
                        result = handler(self._coerce_rpc_request(event_type, data))
                    finally:
                        self._handler_local.active = False
                    self._validate_rpc_response(event_type, result)
                    # 发送响应 (List Push)
                    if reply_to:
                        normalized = self._normalize_payload(result)
                        self.sync_redis.rpush(reply_to, serialize_data({"request_id": request_id, "data": normalized}))
                        self.sync_redis.expire(reply_to, 60)  # 1分钟过期，防止残留
                except Exception as e:
                    print(f"RPC {event_type} 处理失败: {e}")
                    if reply_to:
                        self.sync_redis.rpush(reply_to, serialize_data({"request_id": request_id, "error": str(e)}))
                        self.sync_redis.expire(reply_to, 60)
            else:
                print(f"收到RPC请求 {event_type} 但无处理器")
        else:
            # 订阅 处理
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
                # 只有在既不是RPC也不是订阅时才提示，或者忽略
                if event_type not in self.rpc_handlers:
                    print(f"无事件 {event_type} 的订阅者或RPC处理器，跳过")

    async def _process_event_async(self, event_type: str, publish_data: dict):
        """异步处理事件"""
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
                        serialize_data({"request_id": request_id, "error": str(e)}),
                    )
                    await self.async_redis.expire(reply_to, 60)
                return
            # Server-side timeout check
            timestamp = publish_data.get("timestamp")
            timeout = publish_data.get("timeout")

            if timestamp and timeout:
                if time.time() - timestamp > timeout:
                    print(f"Async RPC请求 {event_type} (ID: {request_id}) 已超时，丢弃处理")
                    return  # Skip processing and response

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
                                                     serialize_data({"request_id": request_id, "data": normalized}))
                        await self.async_redis.expire(reply_to, 60)
                except Exception as e:
                    print(f"Async RPC {event_type} 处理失败: {e}")
                    if reply_to:
                        await self.async_redis.rpush(reply_to,
                                                     serialize_data({"request_id": request_id, "error": str(e)}))
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

    def _listen_loop(self):
        """监听循环，运行在子进程或主线程"""
        if self.sync_redis is None:
            self.sync_redis = SyncRedisClient.get_instance(self.redis_config)

        channel_keys = []
        if self.subscribers:
            channel_keys = [self._channel_key(event_type) for event_type in self.subscribers.keys()]
            pubsub = self.sync_redis.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(*channel_keys)
        else:
            pubsub = None

        # 重新收集 RPC 流
        all_events = set(self.rpc_handlers.keys())
        stream_keys = []
        for event_type in all_events:
            stream_key = self._stream_key(event_type)
            stream_keys.append(stream_key)
            self._ensure_group(stream_key, start_id="$", reset_last_id=True)

        print(f"事件监听已启动 (PID: {multiprocessing.current_process().pid})，订阅: {list(self.subscribers.keys())}，RPC: {list(all_events)}")

        consumer_name = f"consumer-{multiprocessing.current_process().pid}"
        while True:
            pubsub_timeout = 0.05 if stream_keys else 0.001
            if pubsub is not None:
                message = pubsub.get_message(timeout=pubsub_timeout)
                if message and message.get("type") == "message":
                    channel = message.get("channel")
                    payload = message.get("data")
                    event_type = self._event_type_from_channel(channel)
                    if payload:
                        try:
                            publish_data = deserialize_data(payload)
                            self._process_event(event_type, publish_data)
                        except Exception as e:
                            print(f"PubSub Data process error: {e}")

            if not stream_keys:
                if pubsub is None:
                    time.sleep(0.05)
                continue

            streams = {key: ">" for key in stream_keys}
            try:
                messages = self.sync_redis.xreadgroup(
                    self.consumer_group,
                    consumer_name,
                    streams,
                    count=1,
                    block=50
                )
            except redis.exceptions.ResponseError as e:
                print(f"Redis Read Error: {e}")
                time.sleep(1)
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
                        publish_data = deserialize_data(payload)
                        self._process_event(event_type, publish_data)
                    except Exception as e:
                        print(f"Data process error: {e}")

                    self.sync_redis.xack(stream_name, self.consumer_group, message_id)
                    self.sync_redis.xdel(stream_name, message_id)

    def run(self, block: bool = True):
        """启动监听"""
        if block:
            self._listen_loop()
        else:
            listener_thread = multiprocessing.Process(target=self._listen_loop)
            listener_thread.daemon = True
            listener_thread.start()

    async def run_async(self):
        """启动异步监听"""
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
                    publish_data = deserialize_data(payload)
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
                    count=1,
                    block=50
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
                        publish_data = deserialize_data(payload)
                        await self._process_event_async(event_type, publish_data)
                    except Exception as e:
                        print(f"Async Data process error: {e}")

                    await self.async_redis.xack(stream_name, self.consumer_group, message_id)
                    await self.async_redis.xdel(stream_name, message_id)
