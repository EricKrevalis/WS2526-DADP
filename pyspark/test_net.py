import socket
import os
from dotenv import load_dotenv

# Explicitly load .env from the project root (one level up)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
dotenv_path = os.path.join(project_root, '.env')

print(f"Loading .env from: {dotenv_path}")
load_dotenv(dotenv_path)

ip = os.getenv("KAFKA_BROKER_IP")
port = 9092

if not ip:
    print("❌ Error: KAFKA_BROKER_IP not found.")
    print("   -> Check your .env file syntax (Line 10 issue).")
    exit(1)

print(f"Attempting connection to {ip}:{port}...")

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex((ip, port))

if result == 0:
    print(f"✅ Socket connection to {ip} is OPEN.")
else:
    print(f"❌ Connection failed. Error code: {result}")
    if result == 11:
        print("   -> Error 11 (EAGAIN): Resource temporarily unavailable.")
        print("      Try toggling your WiFi or waiting 60 seconds.")
    elif result == 111:
        print("   -> Error 111 (Refused): Server is online, but Kafka is DOWN.")
    elif result == 110:
        print("   -> Error 110 (Timeout): Firewall block. Check Security Groups.")

sock.close()