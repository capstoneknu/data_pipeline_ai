from pydantic import BaseModel, Field

class MissionGenerateResponse(BaseModel):
    """
    Java(Spring Boot) B파트 서버가 기대하는 응답 규격(Contract).
    명세서에 맞춰 데이터 타입의 무결성을 충족하도록 조치.
    """
    user_id: str = Field(..., description="예측 대상 사용자 ID")
    predicted_cbl_kwh: float = Field(..., description="LSTM 예측 피크 시간대 전력 수요량 (kW)")
    mission_target_kwh: float = Field(..., description="ANFIS 산출 DR 감축 목표 전력량 (kW)")
    curtailment_ratio_percent: float = Field(..., description="감축 비율 (%)")
    expected_reward_points: int = Field(..., description="보상 포인트 산출량")
    difficulty: str = Field(..., description="미션 난이도 라벨 (Easy, Medium, Hard)")