from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import router

# 서버 심장 & CORS 방어막
app = FastAPI(
    title="우리집 전기 저금통 코어 API",
    description="LSTM 예측 및 ANFIS 난이도 조절 엔진 API 서버",
    version="1.0.0"
)

# CORS 설정: 팀원의 React Native나 웹뷰에서 API 호출 시 차단당하지 않도록 전부 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # 실무 배포 시에는 특정 도메인으로 좁혀야 함
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 장착
app.include_router(router)

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "ANFIS Server is perfectly running."}