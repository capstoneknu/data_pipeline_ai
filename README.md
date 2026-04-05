# ANFIS 구현 진행중<br>
# 우리집 전기 저금통 제도를 학부 수준에서 구현 (AI & Data Pipeline)

본 레포지토리는 스마트 미터(AMI) 전력 데이터를 수집하고,
딥러닝(LSTM)을 통해 미래 수요를 예측하는 파이프라인입니다.

## 필수 데이터 세팅 안내
본 프로젝트의 원본 데이터는 대용량으로, 드라이브에 압축하여 링크를 공유합니다. <br>
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
# 현재까지 폴더 구조 요약
```
Capstone2026/
 ┣ data/
 ┃  ┣ final_synthetic_ami_data.csv
 ┃  ┣ uci_power.txt
 ┃  ┗ wonju_power.csv
 ┣ .gitignore
 ┣ docker-compose.yml
 ┣ README.md          
 ┣ ai-data-pipeline/
 ┃  ┣ requirements.txt
 ┃  ┣ generate_dr_data.py 
 ┃  ┣ virtual_esp32_sensor.py
 ┃  ┣ ingestion_api.py
 ┃  ┣ data_loader.py
 ┃  ┣ model.py
 ┃  ┗ train.py
 ┗ backend-app-svc/
```
