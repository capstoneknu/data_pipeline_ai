#!/bin/bash

# ===================================================
#   [MSA] Energy Dashboard System Start-up Script
# ===================================================

# 경로 정의 (Ubuntu 환경 맞춤형)
BASE_DIR="$HOME/data_pipeline_ai"
AI_DIR="$BASE_DIR/ai-data-pipeline"
BACKEND_DIR="$HOME/backend_service_client/energy-api" # BASE_DIR과 분리!

# 로그 파일 정의
FASTAPI_LOG="$AI_DIR/api_serving/fastapi.log"
INGESTION_LOG="$AI_DIR/workers/ingestion.log"
MQTT_LOG="$AI_DIR/workers/mqtt_worker.log"
SENSOR_LOG="$AI_DIR/simulators/sensor_traffic.log"
SPRING_LOG="$BACKEND_DIR/springboot.log"

echo "==================================================="
echo "  Starting Energy Dashboard MSA Infrastructure"
echo "==================================================="

# [Phase 1] 인프라 컨테이너 가동
echo "---------------------------------------------------"
echo "[1/6] Starting Infrastructure (Kafka, InfluxDB)..."
cd "$BASE_DIR" || exit
if [ -f "docker-compose.yml" ]; then
    docker-compose up -d
    echo "Wait 10 seconds for DBs to initialize..."
    sleep 10
else
    echo "Error: docker-compose.yml not found in $BASE_DIR"
    exit 1
fi

# 파이썬 가상환경 활성화 (이후 모든 파이썬 실행에 적용)
echo "---------------------------------------------------"
echo "Activating Python Virtual Environment (.venv)..."
if [ -d "$AI_DIR/.venv" ]; then
    source "$AI_DIR/.venv/bin/activate"
else
    echo "Error: Virtual environment (.venv) not found in $AI_DIR"
    exit 1
fi

# [Phase 3] AI 엔진 (FastAPI) 가동
echo "---------------------------------------------------"
echo "[2/6] Starting AI Engine Serving (FastAPI)..."
cd "$AI_DIR" || exit
# 디렉터리 구조에 맞춰 모듈 경로 수정
nohup python -m uvicorn api_serving.main:app --host 0.0.0.0 --port 8000 > "$FASTAPI_LOG" 2>&1 &
echo "FastAPI started. Logs: $FASTAPI_LOG"
sleep 3

# [Phase 4] 비동기 적재 워커 데몬 가동
echo "---------------------------------------------------"
echo "[3/6] Starting Data Ingestion Workers..."
cd "$AI_DIR/workers" || exit
nohup python ingestion_api.py > "$INGESTION_LOG" 2>&1 &
nohup python mqtt_ingestion_worker.py > "$MQTT_LOG" 2>&1 &
echo "Workers started. Logs: $INGESTION_LOG, $MQTT_LOG"
sleep 3

# [Phase 6] Spring Boot 메인 백엔드 데몬 가동
echo "---------------------------------------------------"
echo "[4/6] Starting Spring Boot Main Backend..."
cd "$BACKEND_DIR" || exit
if [ -f "./gradlew" ]; then
    nohup ./gradlew bootRun > "$SPRING_LOG" 2>&1 &
    echo "Spring Boot started. Logs: $SPRING_LOG"
    # 사용자의 요청에 따라 20초 대기
    echo "Waiting 20 seconds for Spring Boot to fully start..."
    sleep 20
else
    echo "Error: gradlew not found in $BACKEND_DIR"
    exit 1
fi

echo "---------------------------------------------------"
echo "[5/6] Verification: Checking Service Status..."
echo "  FastAPI (Port 8000): $(netstat -tpln | grep :8000 | awk '{print $6}')"
echo "  Spring Boot (Port 8085): $(netstat -tpln | grep :8085 | awk '{print $6}')"
echo "---------------------------------------------------"

# [Phase 5] 물리-가상 하이브리드 트래픽 전송 (모든 준비 완료 후)
echo "[6/6] Starting Traffic Simulator (10,000 TPS)..."
cd "$AI_DIR/simulators" || exit
nohup python virtual_esp32_sensor.py > "$SENSOR_LOG" 2>&1 &
echo "Traffic Simulator started. Logs: $SENSOR_LOG"

echo "==================================================="
echo "  All MSA components have been started!"
echo "  Use './stop_msa.sh' to shut down the system."
echo "==================================================="