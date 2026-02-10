import time
import multiprocessing
import os
from event_app_redis import EventApp
from q_log.log_nb import logger

def run_server():
    """Simulates a server process that handles events and RPCs"""
    # Create a distinct app instance for the server
    # Note: Using a unique group name ensures we don't conflict with other consumers if any
    server_app = EventApp(group_name="server_process_group")

    # 1. Register Subscribers (Multi-subscription test)
    @server_app.subscribe("test_topic")
    def handle_topic_1(data):
        logger.info(f"[Server PID {os.getpid()}] Subscriber 1 received: {data}")

    # 2. Register RPC
    @server_app.rpc("test_rpc")
    def handle_rpc(data):
        logger.info(f"[Server PID {os.getpid()}] RPC processing: {data}")
        # Simulate some processing time
        # time.sleep(0.5)
        return f"Processed '{data}' by PID {os.getpid()}"

    logger.info(f"[Server PID {os.getpid()}] Starting event loop...")
    # Blocking run to keep the process alive and listening
    server_app.run(block=True)

def run_client():
    """Simulates a client process that publishes events and calls RPCs"""
    # Client doesn't need a group name if it only publishes/requests, 
    # but providing one doesn't hurt.
    client_app = EventApp(group_name="client_process_group")
    
    # Wait for server to be ready
    logger.info(f"[Client PID {os.getpid()}] Waiting for server to start...")
    time.sleep(3)

    # Warmup: Initialize Redis connection
    logger.info(f"[Client] Warming up Redis connection...")
    # client_app.publish("warmup", "init")

    # Test 1: Publish (Async, Multi-subscriber)
    logger.info(f"\n[Client] Publishing to 'test_topic'...")
    client_app.publish("test_topic", "Hello Distributed World")
    
    # Test 2: RPC (Sync)
    logger.info(f"\n[Client] Calling RPC 'test_rpc'...")
    try:
        start = time.time()
        # Timeout set to 5s
        response = client_app.get("test_rpc", "RPC Request Data", timeout=5.0)
        duration = time.time() - start
        logger.info(f"[Client] RPC Response: {response}")
        logger.info(f"[Client] RPC Duration: {duration:.4f}s")

        start2 = time.time()
        response = client_app.get("test_rpc", "RPC Request Data", timeout=5.0)
        duration = time.time() - start2
        logger.info(f"[Client] RPC Response: {response}")
        logger.info(f"[Client] RPC Duration: {duration:.4f}s")
    except Exception as e:
        logger.info(f"[Client] RPC Failed: {e}")

if __name__ == "__main__":
    logger.info(f"Main Process PID: {os.getpid()}")
    
    # Start the server in a separate process
    server_process = multiprocessing.Process(target=run_server, name="ServerProcess")
    server_process.start()
    
    try:
        # Run client logic in the main process
        run_client()
        
        # Give some time for async logs to appear
        time.sleep(2)
    finally:
        # Cleanup
        logger.info("\nTerminating server...")
        server_process.terminate()
        server_process.join()
        logger.info("Test Finished.")
