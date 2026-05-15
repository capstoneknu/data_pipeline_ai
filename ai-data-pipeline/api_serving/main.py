import logging
import sys
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 분리된 2개의 도메인 라우터를 임포트
from api_serving.router import mission_router, prediction_router
from api_serving.services import ai_wrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [AI-CORE] - %(levelname)s - %(message)s')

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("[System] A파트(AI & Data Pipeline) 코어 서버 기동을 준비합니다...")
    try:
        logging.info("[System] LSTM 예측 모델 및 ANFIS 엔진을 메모리에 적재합니다.")
        ai_wrapper._initialize() # 콜드 스타트 방어
        logging.info("[System] AI 뇌구조 적재 완료. 1만 가구 규모의 트래픽 수신 준비 완료.")
    except AttributeError:
        logging.warning("[System] 명시적 Warm-up 메서드가 없습니다. Lazy-Loading을 수행합니다.")
    except Exception as e:
        logging.critical(f"[System] AI 뇌구조 적재 실패: {e}")
        raise e
        
    yield 
    logging.info("[System] A파트 서버가 안전하게 종료되었습니다. 자원을 반환합니다.")

app = FastAPI(
    title="우리집 전기 저금통 - AI 추론 코어 API",
    description="Java(Spring Boot) 서버의 대용량 배치 요청을 받아 LSTM 수요 예측 및 ANFIS 난이도를 산출하는 내부망 전용 AI 게이트웨이",
    version="2.1.0",
    lifespan=lifespan 
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# 2개의 독립된 라우터 마운트 (결합도 최소화)
app.include_router(mission_router)           
app.include_router(prediction_router)

@app.get("/", tags=["Health Check"])
async def health_check():
    return JSONResponse(
        status_code=200, 
        content={
            "status": "ok", 
            "module": "AI-Data-Pipeline",
            "message": "AI Inference Core is perfectly running."
        }
    )