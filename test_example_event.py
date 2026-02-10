from event_app_redis import EventApp
from q_log.log_nb import logger

app = EventApp()

# --- 1. 发布-订阅模式 (Publish - Subscribe) ---
# 支持多个订阅者监听同一事件
@app.subscribe("user_login")
def log_login(data):
    logger.info(f"[Log] User logged in: {data}")

@app.subscribe("user_login")
def send_welcome_email(data):
    logger.info(f"[Email] Sending welcome email to {data}")

# --- 2. RPC 模式 (Get - RPC) ---
# 请求-响应模式，有返回值
@app.rpc("get_user_info")
def get_user_info(user_id):
    # 模拟查询
    return {"id": user_id, "name": "Alice", "role": "Admin"}

# --- 启动应用 ---
if __name__ == "__main__":
    import time
    # 在后台或单独进程运行 app.run()
    app.run(block=False) 
    time.sleep(1) # 等待监听启动
    
    # 1. 发布事件 (异步，无返回值)
    app.publish("user_login", "user_123")
    
    # 2. 调用 RPC (同步，等待返回值)
    try:
        logger.info("Sending RPC request...")
        user_info = app.get("get_user_info", "user_123", timeout=5.0)
        logger.info(f"RPC Result: {user_info}")
    except TimeoutError:
        logger.info("RPC Timed out")
        
    time.sleep(1) # 等待异步日志打印