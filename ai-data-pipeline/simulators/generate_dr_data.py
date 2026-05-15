import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataFusionGenerator:
    def __init__(self, uci_filename="uci_power.txt", wonju_filename="wonju_power.csv", num_users=10000, days=30):
        current_dir = Path(__file__).parent
        data_dir = current_dir.parent / "data"
        
        self.uci_path = str(data_dir / uci_filename)
        self.wonju_path = str(data_dir / wonju_filename)
        self.output_file = str(data_dir / "final_synthetic_ami_data.csv")
        
        # 1만 가구 볼륨 확정
        self.num_users = num_users
        self.days = days
        self.total_minutes = days * 24 * 60

    def _load_and_process_wonju(self) -> np.ndarray:
        df_w = pd.read_csv(self.wonju_path, encoding='cp949') 
        df_w.columns = df_w.columns.str.strip()
        df_w['per_user_hourly_kwh'] = (df_w['전력사용량(MWh)'] * 1000) / df_w['고객호수']
        hourly_trend = df_w['per_user_hourly_kwh'].head(self.days * 24).values
        minute_trend = np.repeat(hourly_trend, 60) / 60.0 
        return minute_trend.reshape(-1, 1)

    def _load_and_process_uci(self) -> np.ndarray:
        df_u = pd.read_csv(self.uci_path, sep=';', na_values=['?', ''], low_memory=False)
        power_series = df_u['Global_active_power'].ffill().values
        power_slice = power_series[:self.total_minutes]
        normalized_pattern = power_slice / np.mean(power_slice)
        return normalized_pattern.reshape(-1, 1)

    def generate(self, time_chunk_size=1440): # 1440분(1일) 단위로 청크 분할
        logging.info(f"데이터 융합 시작: 총 {self.num_users}가구, {self.days}일치 시계열 데이터 생성")

        # [시공간 동기화] 과거가 아닌 오늘 자정 00:00부터 데이터 시작
        # Spring Boot의 AI 예측 데이터(오늘 자정 기준)와 시간선이 일치하여 회색 점선이 정상 동작
        now_utc = datetime.now(timezone.utc)
        start_date = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        wonju_trend = self._load_and_process_wonju()
        uci_pattern = self._load_and_process_uci()
        fused_base_pattern = wonju_trend * uci_pattern
        
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
            
        header_written = False
        user_ids = [str(i) for i in range(1, self.num_users + 1)]
        
        # 1만 가구의 고유 변동성 계수 (한 번만 메모리에 생성하여 유지)
        user_multipliers = np.random.uniform(0.7, 1.3, size=(1, self.num_users))
        
        # 유저가 아닌 시간 단위로 루프를 돌아 CSV가 시간순으로 작성되도록 설정
        for t_start in range(0, self.total_minutes, time_chunk_size):
            t_end = min(t_start + time_chunk_size, self.total_minutes)
            chunk_timestamps = [start_date + timedelta(minutes=i) for i in range(t_start, t_end)]
            
            # (시간청크 x 1만가구) 매트릭스 연산 
            usage_matrix = fused_base_pattern[t_start:t_end] * user_multipliers
            noise = np.random.normal(0, np.mean(fused_base_pattern) * 0.1, usage_matrix.shape)
            final_usage = np.clip(usage_matrix + noise, 0, None)
            
            df_chunk = pd.DataFrame(final_usage, columns=user_ids)
            df_chunk['timestamp'] = chunk_timestamps
            
            # melt 연산 
            df_long = df_chunk.melt(id_vars=['timestamp'], var_name='user_id', value_name='kwh_usage')
            df_long['kwh_usage'] = df_long['kwh_usage'].astype('float32') # 용량 50% 절감

            # 유저순 묶음을 시간순으로 재정렬
            # 이를 통해 1만 가구가 00:01에 동시에 쏘고, 그다음 00:02에 동시에 쏘는 진짜 스트리밍 환경 구축
            df_long = df_long.sort_values(by=['timestamp', 'user_id'])
            
            # 디스크 쓰기
            df_long.to_csv(self.output_file, mode='w' if not header_written else 'a', 
                           header=not header_written, index=False)
            header_written = True
            
            progress = (t_end / self.total_minutes) * 100
            logging.info(f"시간 동기화율: {t_end}/{self.total_minutes} 진행 중... ({progress:.1f}%)")

        logging.info(f"파이프라인 데이터 생성 완료 [{self.output_file}]")

if __name__ == "__main__":
    np.random.seed(42)
    generator = DataFusionGenerator(num_users=10000, days=30)
    # 하루 단위(1440분) 청크로 나누어 Out of Memory 회피
    generator.generate(time_chunk_size=1440)