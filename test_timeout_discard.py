import time
import multiprocessing
import uuid
import zmq
from event_app import EventApp, serialize_data

def run_server():
    app = EventApp(group_name="server_timeout_test")
    
    @app.rpc("test_timeout_rpc")
    def handle_rpc(data):
        print(f"Server processing: {data}")
        return "Pong"
        
    print("Server started")
    app.run(block=True)

def run_client():
    app = EventApp()
    time.sleep(1)
    print("Sending expired request...")
    request_id = str(uuid.uuid4())
    publish_data = {
        "event_type": "test_timeout_rpc",
        "data": "Expired Data",
        "need_response": True,
        "request_id": request_id,
        "timestamp": time.time() - 10, # 10 seconds ago
        "timeout": 5.0 
    }
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.linger = 0
    socket.connect(app.zmq_config["rpc_endpoint"])
    socket.send(serialize_data(publish_data))
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    events = dict(poller.poll(1000))
    if socket in events:
        socket.recv()
    socket.close()
    print("Expired request sent.")

if __name__ == "__main__":
    p = multiprocessing.Process(target=run_server)
    p.start()
    
    try:
        run_client()
        time.sleep(2) # Wait for server to process
    finally:
        p.terminate()
        p.join()
