#!/bin/bash

# ===================================================
#   [MSA] Energy Dashboard System Status Checker
# ===================================================

# 터미널 출력 컬러 정의 
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo "==================================================="
echo -e "${CYAN}  [MSA] Energy Dashboard System Status Check${NC}"
echo "==================================================="

# ---------------------------------------------------
# 1. 인프라 컨테이너 상태 확인 (Docker)
# ---------------------------------------------------
echo -e "\n${YELLOW}[1] Infrastructure Containers (Docker)${NC}"
echo "---------------------------------------------------"
if [ -x "$(command -v docker)" ]; then
    # 실행 중인 컨테이너 중 프로젝트 관련 DB만 필터링하여 출력
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "mysql|influx|kafka|Names"
else
    echo -e "${RED}Error: Docker is not installed or running.${NC}"
fi

# ---------------------------------------------------
# 2. 애플리케이션 및 데몬 프로세스 체크 함수
# ---------------------------------------------------
check_service() {
    local proc_pattern=$1
    local display_name=$2
    local port=$3
    
    echo -n "• $display_name: "
    
    # 1) PID 존재 여부 확인
    local pid=$(ps -ef | grep "$proc_pattern" | grep -v grep | grep -v "status_msa.sh" | awk '{print $2}' | head -n 1)
    
    # 2) 포트 리스닝 여부 확인 (포트 인자가 있을 때만 실행)
    local port_alive=1
    if [ ! -z "$port" ]; then
        if ss -tuln | grep -q ":$port "; then
            port_alive=1
        else
            port_alive=0
        fi
    fi
    
    # 최종 상태 판정
    if [ ! -z "$pid" ] && [ $port_alive -eq 1 ]; then
        if [ ! -z "$port" ]; then
            echo -e "${GREEN}RUNNING${NC} (PID: $pid | Port: $port)"
        else
            echo -e "${GREEN}RUNNING${NC} (PID: $pid)"
        fi
    else
        echo -e "${RED}STOPPED${NC}"
    fi
}

# ---------------------------------------------------
# 3. 각 컴포넌트별 상태 판정 실행
# ---------------------------------------------------
echo -e "\n${YELLOW}[2] Application Components & Daemons${NC}"
echo "---------------------------------------------------"

# AI 엔진 및 메인 백엔드 (프로세스 패턴, 표시 이름, 포트번호)
check_service "api_serving.main:app" "AI Engine Core (FastAPI)" "8000"
check_service "energy-api" "Main Business Server (Spring Boot)" "8085"

# 비동기 데이터 적재 워커
check_service "ingestion_api.py" "InfluxDB Ingestion Worker" ""
check_service "mqtt_ingestion_worker.py" "MQTT-Kafka Bridge Worker" ""

# 시뮬레이터
check_service "virtual_esp32_sensor.py" "10,000 TPS Traffic Simulator" ""

echo -e "\n==================================================="