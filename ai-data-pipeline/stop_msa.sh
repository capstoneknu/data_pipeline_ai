#!/bin/bash

# ===================================================
#   [MSA] Energy Dashboard System Shutdown Script
# ===================================================

# 경로 정의 (Spring Boot 종료용)
BASE_DIR="$HOME/data_pipeline_ai"
BACKEND_DIR="$HOME/backend_service_client/energy-api"

echo "==================================================="
echo "  Shutting Down Energy Dashboard MSA"
echo "==================================================="

# 1. 파이썬 기반 AI 코어 및 데이터 워커 일괄 종료
echo "[1/4] Stopping Python Daemon Processes..."
pkill -f python
if [ $? -eq 0 ]; then
    echo "  Python processes terminated."
else
    echo "  No Python processes found."
fi

# 2. 자바 기반 Spring Boot 백엔드 종료
echo "[2/4] Stopping Java Daemon Processes (Spring Boot)..."
pkill -f bootRun
if [ $? -eq 0 ]; then
    echo "  Spring Boot process terminated."
else
    echo "  No Spring Boot process found."
fi

# 3. 백그라운드에 숨은 빌드 보조 프로그램(Gradle Daemon) 완전 종료
echo "[3/4] Stopping Gradle Daemon..."
if [ -d "$BACKEND_DIR" ]; then
    cd "$BACKEND_DIR" || exit
    if [ -f "./gradlew" ]; then
        ./gradlew --stop
        echo "  Gradle Daemon stopped."
    else
        echo "  Warning: gradlew not found in $BACKEND_DIR. Cannot stop Gradle Daemon via gradlew."
    fi
else
    echo "  Warning: Backend directory $BACKEND_DIR not found."
fi

# 4. 인프라 컨테이너 종료 (선택 사항 - 데이터 보존을 위해 주석 처리됨)
# echo "[Optional 4/4] Stopping Docker Containers..."
# cd "$BASE_DIR" || exit
# docker-compose down
# echo "  Docker containers stopped."

echo "---------------------------------------------------"
echo "[4/4] Verification: Remaining Processes..."
echo "  Remaining Python:"
ps -ef | grep python | grep -v grep
echo "  Remaining Java:"
ps -ef | grep java | grep -v grep
echo "---------------------------------------------------"

echo "==================================================="
echo "  Shutdown Sequence Completed."
echo "==================================================="