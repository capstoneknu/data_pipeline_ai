# 대용량 데이터 원본파일 다운로드 링크
```
빠른 시일 내 추가예정
구하기 힘든 실제 1분 단위 데이터를 만들기 위해
가상데이터를 만들기보다는 실제 존재하는 데이터를 활용하였음
프랑스 파리 전역 2075259가구의 1분 단위로 측정한 전력 소비 데이터에 
원주시 시간대별 전력 사용량 데이터의 전력 소비 패턴을 융합하여 1,000가구 분량의 실무형 데이터를 합성하였음
```

# 최종 구조 요약
```
Capstone2026/
 ┣ .gitignore
 ┣ docker-compose.yml
 ┣ README.md            <-- (프로젝트 설명 및 대용량 데이터 다운로드 클라우드 링크)
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
