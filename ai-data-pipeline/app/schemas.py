from pydantic import BaseModel, Field

# 데이터 문지기 역할
# 클라이언트(앱)가 이상한 데이터를 보내면 서버 입구에서 컷오프(422 Error)
class MissionRequest(BaseModel):
    user_id: str = Field(..., description="앱 사용자 고유 ID")
    predicted_cbl: float = Field(..., description="LSTM이 예측한 내일 전력량 (kWh)")
    reliability: float = Field(default=0.5, ge=0.0, le=1.0, description="유저 과거 미션 성공률 (0~1)")
    stress: float = Field(default=0.1, ge=0.0, le=1.0, description="전력망 스트레스 지수 (0~1)")

class MissionResponse(BaseModel):
    user_id: str
    predicted_cbl_kwh: float
    mission_target_kwh: float
    curtailment_ratio_percent: float
    expected_reward_points: int
    difficulty: str