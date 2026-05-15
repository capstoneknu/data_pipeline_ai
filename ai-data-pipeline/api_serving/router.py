import logging
import asyncio
import math
from typing import List
from datetime import date
from fastapi import APIRouter, HTTPException, Path, Body, Query
from pydantic import BaseModel, Field

from api_serving.schemas import MissionGenerateResponse 
from api_serving.services import generate_single_mission
from api_serving.services import ai_wrapper # LSTM 모델 래퍼
from api_serving.services import generate_single_mission, get_24h_prediction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [AI-GATEWAY] - %(levelname)s - %(message)s')

# ==========================================
# 도메인 1: 미션 생성 라우터 
# ==========================================
mission_router = APIRouter(prefix="/api/v1/missions", tags=["AI Mission Generation"])

@mission_router.post("/generate/batch", response_model=List[MissionGenerateResponse])
async def generate_dr_mission_batch(
    user_ids: List[str] = Body(..., description="미션을 생성할 대상 사용자 ID 배열 (최대 10,000개)"),
    grid_stress: float = Body(0.90, description="현재 전력망 스트레스 지수")
):
    try:
        logging.info(f"[Batch] {len(user_ids)}가구 규모의 대용량 AI 미션 생성 요청 수신.")
        tasks = [generate_single_mission(uid, grid_stress) for uid in user_ids]
        return await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"[Batch] 대규모 AI 추론 중 오류: {e}")
        raise HTTPException(status_code=500, detail="대규모 AI 추론 엔진 내부 처리 중 오류가 발생했습니다.")

@mission_router.post("/generate/{user_id}", response_model=MissionGenerateResponse)
async def generate_dr_mission_single(
    user_id: str = Path(..., description="예측을 수행할 대상 사용자의 ID")
):
    try:
        return await generate_single_mission(user_id, stress=0.90)
    except Exception as e:
        logging.error(f"[{user_id}] 단일 추론 실패: {e}")
        raise HTTPException(status_code=500, detail="AI 추론 엔진 내부 처리 중 오류가 발생했습니다.")

# ==========================================
# 도메인 2: 전력 수요 예측 라우터
# ==========================================
prediction_router = APIRouter(prefix="/api/v1/predict", tags=["AI Power Prediction"])

class HourlyPrediction(BaseModel):
    hour: int = Field(..., ge=0, le=23, description="시간 (0~23)")
    predicted_kw: float = Field(..., ge=0.0, description="LSTM이 예측한 전력량(kW)")

class PredictionResponse(BaseModel):
    user_id: int
    target_date: str
    predictions: List[HourlyPrediction]

@prediction_router.get("", response_model=PredictionResponse)
async def get_daily_prediction(
    user_id: int = Query(..., description="예측 대상 사용자 ID"),
    target_date: date = Query(..., description="예측 대상 날짜 (YYYY-MM-DD)")
):
    """
    Java(Spring Boot) 백엔드에서 호출하는 24시간 시계열 전력 예측치 반환 API.
    """
    try:
        logging.info(f"[PREDICT] 유저 {user_id}의 {target_date} 일자 LSTM 예측 요청 수신.")
        
        raw_predictions = await get_24h_prediction(str(user_id))
        
        if not raw_predictions:
            raise ValueError("과거 데이터 부족으로 LSTM 모델이 예측값을 도출할 수 없습니다.")

        predictions = []
        
        # LSTM 예측 결과가 15분 단위(96포인트)로 반환될 경우 1시간(24포인트) 단위로 다운샘플링하여 Java 규격 보장
        if len(raw_predictions) == 96:
            for h in range(24):
                chunk = raw_predictions[h * 4 : (h + 1) * 4]
                avg_kw = sum(chunk) / len(chunk)
                predictions.append(HourlyPrediction(hour=h, predicted_kw=round(avg_kw, 4)))
        
        # 이미 1시간 단위(24포인트)로 예측된 경우 직접 매핑
        elif len(raw_predictions) >= 24:
            for h in range(24):
                predictions.append(HourlyPrediction(hour=h, predicted_kw=round(raw_predictions[h], 4)))
        
        else:
            raise ValueError(f"예측된 데이터의 길이({len(raw_predictions)})가 24시간 규격에 미달합니다.")

        return PredictionResponse(
            user_id=user_id,
            target_date=target_date.isoformat(),
            predictions=predictions
        )
        
    except Exception as e:
        logging.error(f"[PREDICT] 예측 데이터 산출 실패: {e}")
        raise HTTPException(status_code=500, detail="LSTM 추론 중 오류가 발생했습니다.")