import sys
import os
import torch
import joblib
import numpy as np
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 파이썬 3.9+ 표준 라이브러리 (KST 시간대 고정용)
from ai_core.model import KHNPSmartDRNet
import logging

# 최상위 루트(ai-data-pipeline)를 파이썬 경로에 동적 주입
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logging.basicConfig(level=logging.INFO, format='%(asctime)s - [ML-ENGINE] - %(levelname)s - %(message)s')

class SmartDRInferenceWrapper:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SmartDRInferenceWrapper, cls).__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        logging.info("AI 추론 뇌구조(LSTM) 초기화 시작...")
        # Docker 컨테이너 내 CPU 자원 경합(Throttling)을 방지하기 위한 최적화
        torch.set_num_threads(1) 

        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        model_path = os.path.join(base_dir, 'ai_core', 'saved_models', 'khnp_dr_best_model.pth')
        scaler_path = os.path.join(base_dir, 'ai_core', 'saved_models', 'scaler.pkl')
        dict_path = os.path.join(base_dir, 'ai_core', 'saved_models', 'user_to_idx.pkl')

        self.device = torch.device("cpu")
        self.kst_zone = ZoneInfo("Asia/Seoul") # 절대적인 KST 기준 시간대 확보

        try:
            self.scaler = joblib.load(scaler_path)
            self.user_to_idx = joblib.load(dict_path)
            actual_num_users = len(self.user_to_idx)

            # 96-in / 96-out 규격 뼈대 생성
            self.model = KHNPSmartDRNet(num_users=actual_num_users, pred_len=96).to(self.device)
            self.model.load_state_dict(torch.load(model_path, map_location=self.device, weights_only=True))
            self.model.eval() # 평가 모드 고정 (Dropout, BatchNorm 비활성화)

            logging.info(f"AI 뇌구조 적재 완료 (24H 예측 스펙, 총 {actual_num_users} 가구 처리 가능)")            
        except Exception as e:
            logging.error(f"AI 뇌구조 적재 실패 (모델/스케일러 누락): {e}")
            raise

    def _generate_time_features(self):
        """ 과거 24시간(96 스텝)에 해당하는 시간 특성 추출 (KST 강제 동기화) """
        # UTC가 아닌 KST 시간으로 현재 시점 계산 (위상 변이 9시간 오차 해결)
        now_kst = datetime.now(self.kst_zone)
        minute_aligned = (now_kst.minute // 15) * 15
        aligned_now = now_kst.replace(minute=minute_aligned, second=0, microsecond=0)
        
        # 현재 시점부터 과거 24시간 전까지 96개의 역산 타임스탬프 생성
        timestamps = [aligned_now - timedelta(minutes=15 * (95 - i)) for i in range(96)]
        
        features = []
        for ts in timestamps:
            # 시간과 요일을 KST 기준으로 정확히 float 변환
            hour_float = ts.hour + (ts.minute / 60.0)
            
            # 주기성 피처(Cyclical Features) 삼각함수 인코딩
            h_sin = np.sin(2 * np.pi * hour_float / 24.0)
            h_cos = np.cos(2 * np.pi * hour_float / 24.0)
            d_sin = np.sin(2 * np.pi * ts.weekday() / 7.0)
            d_cos = np.cos(2 * np.pi * ts.weekday() / 7.0)
            features.append([h_sin, h_cos, d_sin, d_cos])
            
        return np.array(features)

    def predict_24_hours(self, user_id: str, recent_96_kwh: list) -> list:
        # 입력 텐서 차원(Dimension) 무결성 검증
        if len(recent_96_kwh) != 96:
            logging.warning(f"텐서 길이 불일치 (받은 길이: {len(recent_96_kwh)}, 예상: 96). 안전한 Fallback 적용.")
            return [0.0] * 96

        try:
            kwh_array = np.array(recent_96_kwh).reshape(-1, 1)
            kwh_scaled = self.scaler.transform(kwh_array)

            time_features = self._generate_time_features()
            # 전력량(1) + 시간특성(4) = 총 5차원 Feature 결합
            combined_features = np.concatenate((kwh_scaled, time_features), axis=1)

            x_tensor = torch.tensor(combined_features, dtype=torch.float32).unsqueeze(0).to(self.device)
            
            # Cold Start (미학습 유저) 방어 로직 
            if user_id in self.user_to_idx:
                idx = self.user_to_idx[user_id]
            else:
                logging.warning(f"미학습 유저({user_id}) 유입. 기본 임베딩(0)으로 Fallback 추론합니다.")
                idx = 0 
                
            user_tensor = torch.tensor([idx], dtype=torch.long).to(self.device)

            # 성능 극대화: no_grad() 보다 더 엄격하고 빠른 inference_mode() 사용
            with torch.inference_mode():
                pred_scaled = self.model(x_tensor, user_tensor)

            pred_numpy = pred_scaled.cpu().numpy().reshape(-1, 1) 
            pred_actual = self.scaler.inverse_transform(pred_numpy)

            # 출력 배열 변환 및 물리적 무결성(음수 전력량 불가) 보장
            final_predictions = [max(0.0, float(round(val[0], 2))) for val in pred_actual]
            return final_predictions

        except Exception as e:
            logging.error(f"추론 엔진 붕괴 ({user_id}): {e}")
            return [0.0] * 96