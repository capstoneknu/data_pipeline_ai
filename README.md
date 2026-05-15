
# 우리집 전기 저금통 - AI & Data Pipeline
Project Overview
본 레포지토리는 강원특별자치도 2040 탄소중립 실현을 위한 '에지-AI 융합 분산 아키텍처 기반 도민 참여형 수요반응(DR) 플랫폼'의 데이터 파이프라인 및 인공지능(AI) 코어 시스템입니다.
1만 가구 규모의 스마트 미터(AMI) 데이터를 실시간으로 수집·가공하고, LSTM 기반 전력 수요 예측과 ANFIS 기반 동적 난이도 조절(DDA) 엔진을 결합하여 가계 에너지를 효율적으로 관리하는 End-to-End AI 서비스 파이프라인을 구현하였습니다.<br>


# System Architecture & E2E Data Flow
```
[Data Generation & Edge]       [Streaming & Buffering]           [AI Core & Persistence]        [Client Broadcasting]
   (Python Simulator)               (Apache Kafka)                 (FastAPI / InfluxDB)        (Spring Boot / React Native)
                                                                                                          
 ┌──────────────────┐           ┌───────────────────┐           ┌───────────────────────┐        ┌──────────────────────┐
 │  ESP32 Sensors   │ 10,000    │   Kafka Broker    │ 비동기    │  AI Ingestion API     │ 15-Min  │   React Native App   │
 │ (10,000 Nodes)   │ ────▶    │ (power-usage-topic│ ────▶    │ (LSTM / ANFIS Engine) │ ────▶  │  (Real-time Chart &  │
 │  1-Min Interval  │  TPS      │   Rebound Buffer) │ Polling   │   InfluxDB Storage    │ Group  │    DR Gamification)  │
 └──────────────────┘           └───────────────────┘           └───────────────────────┘        └──────────────────────┘
```
 1) 발생 (Generation): 1만 가구의 1분 단위 전력 소비 데이터를 파이썬 기반 가상 ESP32 클러스터가 생성합니다.  
 2) 완충 (Buffering): DR 이벤트 종료 시 발생하는 대규모 리바운드 피크(Rebound Peak) 부하를 방어하기 위해 Apache Kafka가 최대 605MB/s의 처리량으로 데이터를 흡수합니다.  
 3) 예측 및 조절 (AI & DDA): LSTM 모델이 고객기준부하(CBL)를 90% 이상의 정밀도(MAPE 10% 이하)로 예측하고, ANFIS 엔진이 개인화된 미션 난이도를 산출합니다.  
 4) 표출 (Visualization): 백엔드를 거친 데이터는 React Native 앱의 SVG 차트 규격(96슬롯)에 맞춰 시각화되며, E2E 스트리밍을 완성합니다. <br>

# 아키텍처 고도화 및 트러블슈팅 (Key Refinements)
초기 기획에서 한 단계 전진하여, 트래픽 처리와 시공간 데이터 무결성을 확보하기 위해 다음과 같이 아키텍처 고도화하였습니다.
1) 시뮬레이션 규모 확장 및 Cloud GPU 기반 AI 학습 (Google Colab)
- 변경 사항: 프랑스 파리 UCI 데이터와 강원 원주시 데이터를 융합한 합성 데이터의 규모를 1,000가구에서 10,000가구(약 4.3억 건)의 볼륨으로 증가시켰습니다.
- 문제: 합성된 데이터의 규모를 증가시키면서, 로컬 터미널 환경에서 모델(train.py) 학습 시 심각한 병목(OOM 및 장시간 멈춤 현상)이 발생했습니다.
- 해결: 로컬 학습을 과감히 배제하고, Google Colab 환경으로 마이그레이션하여 Cloud GPU(A100/T4)를 활용했습니다. 대규모 분산 학습을 성공적으로 완료한 후, 추출된 가중치(khnp_dr_best_model.pth)와 스케일러(scaler.pkl) 객체만을 로컬의 saved_models 디렉터리로 이관하여 Serving 속도를 극대화하였습니다.<br>

2) Time-Warp 스트리밍 엔진 구현 (시공간 동기화)
- 변경 사항: 기존 유저(User) 단위로 묶여서 발송되던 Pandas melt 데이터 정렬 방식을 시간순(Timestamp) 정렬로 개편하고, 전송 속도(target_tps)를 10,000으로 설정했습니다.
- 타당성: 1만 가구의 1분 치 데이터를 1초 만에 브로커로 쏘아 올림으로써 "현실 세계의 1초 = 시뮬레이션 세상의 1분"이라는 Time-Warp 물리 법칙을 구축했습니다. 이를 통해 발표 시연 시 24시간을 기다리지 않고도 하루 단위의 DR 이벤트 흐름을 관찰할 수 있습니다.<br>

3) 15분 단위(96슬롯) 데이터 다운샘플링 규격 확립
- 변경 사항: 1분 단위 원시(Raw) 데이터를 백엔드에서 시간당 4포인트(15분 단위), 하루 총 96개의 슬롯 배열(hourlyActual)로 다운샘플링하여 앱으로 브로드캐스트합니다.
- 타당성: React Native 프론트엔드의 SVG 꺾은선 그래프 렌더링 한계를 방어하고 UI 프레임 드랍을 막기 위함입니다. 1분 단위 점 1,440개를 화면에 그리는 오버헤드를 제거하고, 앱의 X축 렌더링 공식과 데이터 길이를 동기화하였습니다.<br>

4) 분산 환경의 데이터 오염 (Data Corruption) 차단
- 변경 사항: Kafka Consumer 단에서 존재하지 않는 유저의 데이터를 단일 유저(User 1)에게 몰아넣어 합산 누적량이 6,888kWh로 폭주하던 결함을 도려내고, 매핑된 정확한 유저의 데이터만 수용하도록 Ingestion 필터를 교정했습니다.
- 타당성: 대규모 트래픽 발생 시, 매핑되지 않은 잔여 가구 데이터는 안전하게 폐기하고, 실제 타겟 유저의 데이터만 RDB에 적재함으로써 데이터 파이프라인의 무결성을 확보했습니다.<br><br>

# 필수 데이터 세팅 안내
원본 데이터는 대용량(CSV/TXT)으로 제공되므로, 드라이브에 압축하여 링크를 공유합니다. <br>
코드를 실행하기 전, 반드시 아래 절차를 통해 데이터를 로컬에 세팅해 주세요.

<br>**[데이터 다운로드 링크1 (Google Drive)]**<br>
https://drive.google.com/file/d/1FAhj9jCu8ryB4thFB-vR_AtOtgGcFJkq/view?usp=drive_link  

<br>**[세팅 절차]**<br>
1. 위 링크에서 `power_dataset_v1.zip` 파일을 다운로드합니다.
2. 프로젝트 최상단 경로에 `data/` 폴더를 생성합니다.
3. 다운받은 압축 파일을 해제하여 `data/` 폴더 안에 넣습니다.<br><br>

<br>**[데이터 다운로드 링크2 (Google Drive)]**<br>
https://drive.google.com/drive/folders/1qet8-LPyzRVhxsRTOCw0b9KRusf486kN?usp=drive_link
<br>**[세팅 절차]**<br>
4. CAPSTONE2026_AI/saved_models/내부 3개의 파일(.pth, .pkl)을 로컬의 ai-data-pipeline/ai_core/saved_models/ 경로에 덮어쓰기 합니다.

<br>**[데이터 설명]**<br>
구하기 힘든 실제 1분 단위 전력 소비 데이터를 얻기 위해 가상데이터를 만들기보다는 실제 존재하는 데이터를 활용하였습니다.

프랑스 파리 전역 2075259가구의 1분 단위로 측정한 전력 소비 데이터에, 원주시 시간대별 전력 사용량 데이터의 전력 소비 패턴을 융합하여<br>
1,000가구 분량의 실무형 데이터를 합성하였습니다.
<br><br>

# 시스템 구동 절차 (Execution Guide)
본 시스템은 다수의 분산 노드와 파이프라인으로 구성되어 있습니다. 의존성 충돌 및 파싱 에러를 방지하기 위해, 두 개의 터미널(VS Code) 창을 띄우고 아래의 순서와 경로를 정확히 지켜 실행합니다.<br>

<br>============================================================<br>
[Terminal 1] 
#Phase 1: 인프라 컨테이너 가동<br>
Kafka 브로커 및 데이터베이스를 가장 먼저 가동합니다.
경로: 프로젝트 최상단 (CAPSTONE2026)
docker-compose up -d
<br>============================================================<br>
[Terminal 2] 
#Phase 2: 시공간 동기화 데이터 합성<br>
과거의 오염된 데이터를 버리고, 시간순으로 정렬된 1만 가구 데이터를 새로 뽑아냅니다. (※ 1회만 실행하며, 생성된 CSV는 엑셀로 절대 열지 마십시오. 타임스탬프 포맷이 오염됩니다.)
CAPSTONE2026/ai-data-pipeline/simulators> python generate_dr_data.py
<br>============================================================<br>
[Terminal 3] 
#Phase 3: AI API Serving 기동<br>
B파트(Spring Boot)의 예측 데이터 요청을 수용하기 위해 FastAPI 서버를 켭니다.
CAPSTONE2026/ai-data-pipeline> uvicorn api_serving.main:app --host 0.0.0.0 --port 8000
<br>============================================================<br>
[Terminal 4 & 5] 
#Phase 4 & 5: 스트리밍 및 Ingestion 워커 기동<br>
① 데이터 발사 시작 (Terminal 4)
CAPSTONE2026/ai-data-pipeline/simulators> python virtual_esp32_sensor.py<br>

② 수집 워커 기동 (Terminal 5)
CAPSTONE2026/ai-data-pipeline/workers> python ingestion_api.py
<br>============================================================<br>


# Directory Structure
본 레포지토리는 모델 연구, API 서빙, 데이터 생성, 인프라 관리를 명확히 분리하였습니다.<br>
```
CAPSTONE2026/
├── ai-data-pipeline/
│   ├── ai_core/                        # [AI 모델 훈련 및 추론 코어 로직]
│   │   ├── saved_models/               # Colab에서 훈련 완료 후 이관된 모델 산출물
│   │   │   ├── khnp_dr_best_model.pth  # 학습된 LSTM 최적 가중치 파일
│   │   │   ├── scaler.pkl              # 데이터 정규화 스케일러
│   │   │   └── user_to_idx.pkl         # 가구별 고유 인덱스 맵핑
│   │   ├── anfis_engine.py             # 적응형 뉴로-퍼지 기반 동적 미션 난이도 조절 엔진
│   │   ├── data_loader.py              # InfluxDB/CSV 대용량 데이터 전처리 모듈
│   │   ├── db_client.py                # 시계열 데이터베이스(InfluxDB) 연결 및 I/O 클라이언트
│   │   ├── model.py                    # PyTorch 기반 LSTM 네트워크 아키텍처 정의
│   │   └── train.py                    # 모델 학습 파이프라인 스크립트
│   │
│   ├── api_serving/                    # [FastAPI 기반 AI 모델 서빙/라우팅 계층]
│   │   ├── main.py                     # API 서버 진입점 및 애플리케이션 초기화
│   │   ├── ml_inference.py             # 실시간 요청에 대한 LSTM/ANFIS 추론 수행 모듈
│   │   ├── router.py                   # B파트(Spring Boot) 연동 엔드포인트 정의
│   │   ├── schemas.py                  # Pydantic 기반 Request/Response DTO 정의
│   │   └── services.py                 # API 비즈니스 로직 및 예외 처리
│   │
│   ├── data/                           # (Local) 합성된 1만 가구의 원시/가공 CSV 보관소
│   │
│   ├── notebooks/                      # [클라우드 연구 및 실험 환경]
│   │   └── DR_24H_Model_Training.ipynb # (Google Colab용) 대규모 분산 학습 주피터 노트북
│   │
│   ├── simulators/                     # [가상 센서 및 데이터 파이프라인 발생기]
│   │   ├── bulk_ingestion.py           # 초기 학습을 위해 InfluxDB에 대규모 데이터를 밀어넣는 배치 로더
│   │   ├── generate_dr_data.py         # 1만 가구 1분 단위 시계열 데이터 합성 및 시간순 정렬기
│   │   └── virtual_esp32_sensor.py     # 생성된 데이터를 Kafka로 쏘아 올리는 실시간 스트리밍 에뮬레이터
│   │
│   ├── stress_test/                    # [시스템 부하 및 안정성 한계 검증]
│   │   ├── demo_live_sensor.py         # 시연용 소규모 실시간 센서 테스트
│   │   └── stress_test_simulator.py    # 목표 TPS를 초과하는 극한의 더미 트래픽 발생기
│   │
│   ├── workers/                        # [비동기 백그라운드 데이터 수집 워커]
│   │   ├── ingestion_api.py            # 센서 데이터 1차 필터링 및 파싱 게이트웨이
│   │   └── mqtt_ingestion_worker.py    # MQTT 브로커와 통신하는 구독/발행 워커
│   │
│   └── requirements.txt                # Python 프로젝트 패키지 의존성 명세
│
├── docker-compose.yml                  # Kafka, Zookeeper, InfluxDB 등 인프라 컨테이너 구성 파일
└── README.md                           # 현재 문서
```
