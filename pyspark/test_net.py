import socket
import os
from dotenv import load_dotenv

load_dotenv()
ip = os.getenv("KAFKA_BROKER_IP")
port = 9092

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex((ip, port))

if result == 0:
    print(f"✅ Socket connection is open.")
else:
    print(f"❌ Connection failed. Error code: {result}")
sock.close()