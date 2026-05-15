import sys
import os
import logging
import asyncio
from typing import List, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai_core.anfis_engine import DREnergyFuzzyEngine
from ai_core.db_client import PowerDBClient
from api_serving.ml_inference import SmartDRInferenceWrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SERVICE] - %(levelname)s - %(message)s')

# 싱글턴 엔진 인스턴스 (메모리 절약)
engine = DREnergyFuzzyEngine()
db_client = PowerDBClient()
ai_wrapper = SmartDRInferenceWrapper()

async def generate_single_mission(user_id: str, stress: float) -> dict:
    """
    단일 유저 추론을 위한 AI 비즈니스 로직. 
    블로킹 I/O(DB 조회, AI 추론)를 스레드풀로 넘겨 비동기 이벤트 루프 멈춤 현상 방어.
    """
    # 1. 시계열 DB 조회
    recent_96_kwh = await asyncio.to_thread(db_client.get_recent_history, user_id, limit=96)
    
    # 2. LSTM 추론
    predictions = await asyncio.to_thread(ai_wrapper.predict_24_hours, user_id, recent_96_kwh)
    predicted_peak = max(predictions) if predictions else 0.0

    # 3. ANFIS 생성
    return await asyncio.to_thread(
        engine.generate_mission, 
        user_id=user_id, 
        predicted_cbl=predicted_peak, 
        reliability=0.85, 
        stress=stress
    )

async def get_24h_prediction(user_id: str) -> List[float]:
    """
    [추가된 로직] Java 백엔드의 예측 데이터 동기화를 위한 순수 추론 로직.
    DB에서 과거 24시간(96포인트)의 컨텍스트를 조회하여 LSTM 모델에 주입하고,
    순수한 미래 예측치 배열을 반환합니다.
    """
    # 1. 시계열 DB에서 모델 추론에 필요한 과거 컨텍스트 윈도우(Look-back) 조회
    recent_96_kwh = await asyncio.to_thread(db_client.get_recent_history, user_id, limit=96)
    
    if not recent_96_kwh:
        logging.warning(f"[{user_id}] DB에 과거 전력 기록이 존재하지 않아 Baseline을 추론할 수 없습니다.")
        return []

    # 2. LSTM 기반의 24시간 예측 연산 수행
    predictions = await asyncio.to_thread(ai_wrapper.predict_24_hours, user_id, recent_96_kwh)
    
    return predictions