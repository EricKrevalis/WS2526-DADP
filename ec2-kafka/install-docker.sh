#!/bin/bash
# =============================================================================
# install-docker.sh - Docker + Swap Setup for Ubuntu EC2 (t3.micro)
# Run this script after SSH'ing into your EC2 instance
# =============================================================================

set -e  # Exit on any error

echo "=========================================="
echo "  Docker + Swap Setup for Kafka EC2"
echo "=========================================="

# -----------------------------------------------------------------------------
# Step 1: Create Swap Space (Required for t3.micro with 1GB RAM)
# -----------------------------------------------------------------------------
echo ""
echo "[1/4] Setting up 1GB swap space..."

if [ -f /swapfile ]; then
    echo "  Swap already exists, skipping..."
else
    sudo fallocate -l 1G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "  ✓ Swap space created and enabled"
fi

# Show memory status
echo "  Memory status:"
free -h

# -----------------------------------------------------------------------------
# Step 2: Install Docker Prerequisites
# -----------------------------------------------------------------------------
echo ""
echo "[2/4] Installing Docker prerequisites..."

sudo apt-get update
sudo apt-get install -y ca-certificates curl

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# -----------------------------------------------------------------------------
# Step 3: Add Docker Repository
# -----------------------------------------------------------------------------
echo ""
echo "[3/4] Adding Docker repository..."

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

# -----------------------------------------------------------------------------
# Step 4: Install Docker
# -----------------------------------------------------------------------------
echo ""
echo "[4/4] Installing Docker..."

sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Allow ubuntu user to run docker without sudo
sudo usermod -aG docker ubuntu

# -----------------------------------------------------------------------------
# Done!
# -----------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  ✓ Installation Complete!"
echo "=========================================="
echo ""
echo "IMPORTANT: You must logout and login again for Docker permissions to work!"
echo ""
echo "Run these commands:"
echo "  1. exit"
echo "  2. SSH back in"
echo "  3. docker --version  (to verify)"
echo "  4. cd kafka-server && docker compose up -d"
echo ""

