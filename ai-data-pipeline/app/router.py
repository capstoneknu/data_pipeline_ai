from fastapi import APIRouter, HTTPException
from app.schemas import MissionRequest, MissionResponse
from app.services import generate_mission_service

# API 엔드포인트
# 팀원이 호출할 실제 주소를 연결

router = APIRouter(prefix="/api/v1/missions", tags=["DR Missions"])

@router.post("/generate", response_model=MissionResponse)
async def create_dr_mission(request: MissionRequest):
    try:
        # 서비스 레이어 호출
        mission_data = await generate_mission_service(
            user_id=request.user_id,
            cbl=request.predicted_cbl,
            rel=request.reliability,
            stress=request.stress
        )
        return mission_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mission generation failed: {str(e)}")