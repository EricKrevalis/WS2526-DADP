# Kafka EC2 Setup Guide

This folder contains everything needed to run Kafka on an EC2 instance for streaming S3 data.

## Architecture

```
Lambda (every 10 min) → S3 Bucket → Kafka Connect → Kafka Topics
                                                          ↓
                                            Your Laptop (PySpark)
```

## Prerequisites

- EC2 instance running Ubuntu 24.04 (t3.micro or t3.small)
- Security Group with ports: 22 (SSH), 9092 (Kafka), 8083 (Connect)
- AWS credentials with S3 read access

---

## Step 1: Install Docker on EC2

SSH into your EC2 instance:

```bash
ssh -i "your-key.pem" ubuntu@YOUR_EC2_PUBLIC_IP
```

Copy and run the install script:

```bash
# Create the install script
nano install-docker.sh
# (paste contents from install-docker.sh, save with Ctrl+X, Y, Enter)

# Make executable and run
chmod +x install-docker.sh
./install-docker.sh
```

**IMPORTANT**: After installation, logout and login again:

```bash
exit
# Then SSH back in
```

Verify Docker works:

```bash
docker --version
```

---

## Step 2: Configure Kafka

Create the Kafka server folder:

```bash
mkdir ~/kafka-server
cd ~/kafka-server
nano docker-compose.yaml
```

Paste the contents of `docker-compose.yaml` from this folder.

**IMPORTANT**: Replace these placeholders:
- `YOUR_EC2_PUBLIC_IP` → Your actual EC2 public IP (e.g., `54.123.45.67`)
- `${AWS_ACCESS_KEY_ID}` → Your AWS access key
- `${AWS_SECRET_ACCESS_KEY}` → Your AWS secret key

You can either:
1. Replace them directly in the file, OR
2. Create a `.env` file:

```bash
nano .env
```

```
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=abc123...
```

---

## Step 3: Start Kafka

```bash
cd ~/kafka-server
docker compose up -d
```

Wait 2-3 minutes for everything to start. Check status:

```bash
docker ps
```

You should see 3 containers: `zookeeper`, `broker`, `connect`

Check Connect logs (wait for "Kafka Connect started"):

```bash
docker logs connect -f
# Press Ctrl+C to exit
```

---

## Step 4: Register the S3 Connector

From your **local Windows machine**:

1. Edit `s3-source-connector.json`:
   - Set `s3.bucket.name` to your bucket name

2. Run the registration script:

```powershell
cd ec2-kafka
.\register-connector.ps1 -EC2_IP "YOUR_EC2_PUBLIC_IP"
```

---

## Step 5: Verify Data is Flowing

SSH into EC2 and check:

```bash
# List topics (should see raw-traffic, raw-weather, raw-db)
docker exec broker kafka-topics --list --bootstrap-server localhost:29092

# View some messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic raw-traffic \
  --from-beginning \
  --max-messages 5
```

---

## Useful Commands

| Action | Command |
|--------|---------|
| Start Kafka | `docker compose up -d` |
| Stop Kafka | `docker compose down` |
| View logs | `docker logs broker -f` |
| Restart Connect | `docker restart connect` |
| Check memory | `free -h` |

---

## Troubleshooting

### Connect container keeps restarting
- Check logs: `docker logs connect`
- Usually means not enough memory → verify swap is enabled: `free -h`

### Cannot connect from laptop
- Check Security Group allows your IP on port 9092 and 8083
- Verify `KAFKA_ADVERTISED_LISTENERS` has your EC2 public IP

### Connector not finding S3 files
- Check AWS credentials are correct
- Verify bucket name and path in connector config
- Check Connect logs for S3 errors

---

## Cost Management

Stop EC2 when not using it:

```bash
# From AWS Console, or:
aws ec2 stop-instances --instance-ids i-xxxxx
```

**Note**: Public IP changes when you stop/start. You'll need to update:
1. `docker-compose.yaml` → `KAFKA_ADVERTISED_LISTENERS`
2. Your PySpark connection string

Consider using an Elastic IP ($3.65/month) if you want a fixed IP.

