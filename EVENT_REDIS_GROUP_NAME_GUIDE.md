# EventApp 中 `group_name` 详解

`group_name` 是基于 Redis Stream 的“消费者组”（Consumer Group）概念。理解它对于构建分布式系统（负载均衡 vs 广播）至关重要。

## 1. 核心原则

*   **客户端 (Client)**：若只负责发布消息或发起调用，`group_name` 对其 **无效**（可忽略）
*   **服务端 (Server)**：负责监听和处理消息，`group_name` 决定了消息的分发策略，(非纯客户端，请参考此)

## 2. 两种服务模式

当有多个服务端实例（Server A, Server B...）监听同一个事件时，`group_name` 的命名决定了它们是“竞争关系”还是“独立关系”。

### 模式 A：负载均衡 (Load Balancing) —— 竞争消费

如果你希望多个服务进程**一起干活，分摊压力**。

*   **配置**：所有服务实例使用 **相同** 的 `group_name`。
*   **行为**：一条消息 **只会被其中一个** 服务处理。Redis 内部轮询分发。
*   **场景**：
    *   RPC 服务集群（如：订单处理、AI 推理服务）。
    *   耗时任务队列。

```python
# Server A (订单处理-机器 1)
app = EventApp(group_name="worker_cluster") 
@app.rpc("process_order")
def process(data): ...

# Server B (订单处理-机器 2)
app = EventApp(group_name="worker_cluster") # 名字必须相同
@app.rpc("process_order")
def process(data): ...

# 结果：Client 发送 10 个请求，A 和 B 各处理约 5 个。
```

### 模式 B：广播 (Fan-out) —— 独立消费

如果你希望多个服务进程**各自独立，都能收到**。

*   **配置**：不同服务实例使用 **不同** 的 `group_name`。
*   **行为**：一条消息会被 **每个组都处理一次**（组间互不影响）。
*   **场景**：
    *   发布/订阅模式。
    *   旁路系统（如：一个服务记日志，另一个服务发通知）。

```python
# Server A (日志服务)
app = EventApp(group_name="logger_service") 
@app.subscribe("user_login")
def log(data): ...

# Server B (邮件服务)
app = EventApp(group_name="email_service") # 名字必须不同
@app.subscribe("user_login")
def email(data): ...

# 结果：Client 发布 "user_login"，A 和 B 都会收到完整消息。
```

## 3. 总结表

| 角色 | 是否需要 group_name | 命名规则 | 作用 |
| :--- | :--- | :--- | :--- |
| **Client (发布者)** | 不需要 | 任意 (忽略) | 无作用。RPC 响应通过临时读取获取，不走消费者组。 |
| **Server (集群模式)** | **需要** | **相同** | **负载均衡**：多台机器共同处理一堆任务。 |
| **Server (独立服务)** | **需要** | **不同** | **广播**：不同业务系统各自订阅同一份数据。 |

## 4. 底层原理

在 Redis Stream 中，`group_name` 用于标识一个消费进度。
*   **同组消费者**：共享同一个进度游标（`last_delivered_id`），所以消息不会重复发给组内成员。
*   **不同组消费者**：拥有各自独立的进度游标，互不干扰。
