# 우리집 전기 저금통 제도 구현 (AI & Data Pipeline)
AI 기반 스마트 그리드 수요반응(DR) 서비스 플랫폼
본 프로젝트는 AMI(스마트 미터) 데이터를 실시간으로 수집·가공하고, LSTM 기반의 전력 수요 예측과 ANFIS 기반의 지능형 미션 제어를 결합하여 
가계 에너지를 효율적으로 관리하는 End-to-End AI 서비스 파이프라인입니다.<br>

본 레포지토리는 스마트 미터(AMI) 전력 데이터를 수집하고,
딥러닝(LSTM)을 통해 미래 수요를 예측하는 파이프라인입니다.<br>

# System architecture
- Data Ingestion: 가상 ESP32 센서가 Kafka/FastAPI를 통해 실시간 전력 데이터를 스트리밍
- AI Engine: LSTM: 가구별 과거 소비 패턴을 분석하여 향후 15~60분 내 수요 예측(CBL 산출)
- ANFIS (In Progress): 퍼지 추론 엔진을 통해 예측된 수요 대비 최적의 에너지 절감 미션 및 보상 수준 결정
- Backend API: FastAPI 기반의 마이크로서비스 아키텍처(MSA) 구조로 AI 모델의 추론 결과를 모바일로 중계
- Mobile App: React Native(Expo) 기반 UI를 통해 사용자에게 실시간 미션 알림 및 에너지 통계 제공<br>

# Data Pipeline & Synthesis
대규모 데이터를 시뮬레이션하기 위해 다음과 같은 전략을 사용하였습니다.
- Dataset: UCI 전력 소비 데이터(Paris) + 강원권 원주 지역 전력 데이터 융합.
- Strategy: 글로벌 표준 소비 패턴에 국내 지역적 전력 소모 특성을 주입하여 1,000가구 분량의 실무형 고해상도(1min interval) AMI 데이터를 합성.
- Storage: InfluxDB(시계열 데이터베이스)를 통한 초단위 데이터 고속 저장 및 인덱싱.<br>

## 필수 데이터 세팅 안내
원본 데이터는 대용량(CSV/TXT)으로 제공되므로, 드라이브에 압축하여 링크를 공유합니다. <br>
코드를 실행하기 전, 반드시 아래 절차를 통해 데이터를 로컬에 세팅해 주세요.

<br>**[데이터 다운로드 링크 (Google Drive)]**<br>
https://drive.google.com/file/d/1FAhj9jCu8ryB4thFB-vR_AtOtgGcFJkq/view?usp=drive_link  

<br>**[세팅 절차]**
1. 위 링크에서 `power_dataset_v1.zip` 파일을 다운로드합니다.
2. 프로젝트 최상단 경로에 `data/` 폴더를 생성합니다.
3. 다운받은 압축 파일을 해제하여 `data/` 폴더 안에 넣습니다.

<br>**[데이터 설명]**<br>
구하기 힘든 실제 1분 단위 전력 소비 데이터를 얻기 위해 가상데이터를 만들기보다는 실제 존재하는 데이터를 활용하였습니다.

프랑스 파리 전역 2075259가구의 1분 단위로 측정한 전력 소비 데이터에, 원주시 시간대별 전력 사용량 데이터의 전력 소비 패턴을 융합하여<br>
1,000가구 분량의 실무형 데이터를 합성하였습니다.
<br><br>
## Project Structure
본 레포지토리는 마이크로서비스 아키텍처(MSA) 및 관심사 분리(SoC) 원칙을 준수하여, AI 데이터 파이프라인과 계층형 백엔드 API 서비스로 구성되어 있습니다.
```
Capstone2026/
 ┣ data/                                 # 합성 AMI 데이터셋 (Google Drive 링크 참조)
    ┣ final_synthetic_ami_data.csv
    ┣ uci_power.txt
    ┗ wonju_power.csv
 ┣ .gitignore
 ┣ docker-compose.yml                    # Kafka, InfluxDB, Redis 등 인프라 컨테이너 설정
 ┣ README.md          
 ┗ ai-data-pipeline/                     # AI 코어 및 데이터 수집부
   ┣ app/                                # FastAPI 기반 백엔드 서비스 (Layered Architecture)
   ┃ ┣ __init__.py                       # 패키지 초기화 명세
   ┃ ┣ main.py                           # 백엔드 애플리케이션 진입점 (CORS 세팅 및 라우터 마운트)
   ┃ ┣ router.py                         # API 엔드포인트 정의 (프론트엔드 통신용 Controller 역할)
   ┃ ┣ schemas.py                        # Pydantic 기반 Request/Response 데이터 검증 모델 (DTO)
   ┃ ┗ services.py                       # AI 모델 추론 결과 연동 및 가공
   ┣ requirements.txt                    # Python 의존성 패키지 명세서
   ┣ generate_dr_data.py 
   ┣ virtual_esp32_sensor.py             # 실시간 데이터 시뮬레이터
   ┣ ingestion_api.py                    # 데이터 수집 게이트웨이
   ┣ data_loader.py                      # 대용량 시계열 데이터 배치 로더
   ┣ model.py                            # LSTM & ANFIS 모델 아키텍처
   ┗ train.py                            # 분산 학습 파이프라인
```
