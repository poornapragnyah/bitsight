#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Bitcoin Analysis Pipeline...${NC}"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check for required commands
if ! command_exists docker; then
    echo -e "${RED}Error: Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command_exists docker-compose; then
    echo -e "${RED}Error: Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

if ! command_exists python3; then
    echo -e "${RED}Error: Python 3 is not installed. Please install Python 3 first.${NC}"
    exit 1
fi

# Create required directories
echo -e "${YELLOW}Creating required directories...${NC}"
mkdir -p logs
mkdir -p drivers

# Start Docker containers
echo -e "${YELLOW}Starting Docker containers...${NC}"
docker-compose up -d

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 10

# Create Python virtual environment
echo -e "${YELLOW}Creating Python virtual environment...${NC}"
python3 -m venv bitsight

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source bitsight/bin/activate

# Install Python dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
pip install -r requirements.txt

# Start Kafka consumer in background
echo -e "${YELLOW}Starting Kafka consumer...${NC}"
python kafka/persist_db_consumer.py > logs/kafka_consumer.log 2>&1 &

# Start WebSocket server in background
echo -e "${YELLOW}Starting WebSocket server...${NC}"
python websocket_server.py > logs/websocket_server.log 2>&1 &

# Start Spark processing (if needed)
echo -e "${YELLOW}Starting Spark processing...${NC}"
# Add your Spark job startup command here

# Open dashboard in default browser
echo -e "${YELLOW}Opening dashboard...${NC}"
xdg-open dashboard.html 2>/dev/null || open dashboard.html 2>/dev/null || echo "Please open dashboard.html manually in your browser"

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${YELLOW}Services running:${NC}"
echo -e "1. PostgreSQL database"
echo -e "2. Kafka consumer"
echo -e "3. WebSocket server"
echo -e "4. Dashboard (in browser)"

echo -e "\n${YELLOW}To stop all services, run:${NC}"
echo -e "docker-compose down"
echo -e "pkill -f 'python kafka/persist_db_consumer.py'"
echo -e "pkill -f 'python websocket_server.py'" 