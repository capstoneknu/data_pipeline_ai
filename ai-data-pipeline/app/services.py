import sys
import os
# 비즈니스 로직 및 병목 방어
# CPU 자원 소모가 큰 ANFIS 연산을 메인 스레드에서 분리하여 서버 멈춤을 방지

# 상위 폴더의 anfis_engine을 가져오기 위한 경로 세팅
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starlette.concurrency import run_in_threadpool
from anfis_engine import DREnergyFuzzyEngine

# 싱글톤(Singleton) 패턴: 서버가 켜질 때 엔진을 한 번만 메모리에 올림
engine = DREnergyFuzzyEngine()

async def generate_mission_service(user_id: str, cbl: float, rel: float, stress: float) -> dict:
    """ANFIS 엔진 연산을 스레드풀로 던져 비동기(Async) 병목을 막는 래퍼 함수"""
    # CPU 집약적인 퍼지 연산을 별도 스레드에서 실행
    result = await run_in_threadpool(
        engine.generate_mission,
        user_id=user_id,
        predicted_cbl=cbl,
        reliability=rel,
        stress=stress
    )
    return result