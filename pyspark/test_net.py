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
    print(f"✅ Success! Connected to {ip}:{port}")
else:
    print(f"❌ Failed! Port {port} is closed/blocked on {ip}. Error code: {result}")
    print("ACTION: Go to AWS Console -> Security Groups -> Add Inbound Rule for Port 9092 (Source: 0.0.0.0/0 temporarily)")
sock.close()