import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataFusionGenerator:
from pathlib import Path

class DataFusionGenerator:
    def __init__(self, uci_filename="uci_power.txt", wonju_filename="wonju_power.csv", num_users=1000, days=30):
        # 1. 현재 이 스크립트(generate_dr_data.py)가 있는 폴더의 절대 경로를 찾음
        current_dir = Path(__file__).parent
        
        # 2. 거기서 한 칸 위로 올라간 뒤(..), data 폴더로 들어감
        data_dir = current_dir.parent / "data"
        
        # 3. 경로 합성 (어디서 터미널 명령어를 치든 무조건 올바른 절대경로가 됨)
        self.uci_path = str(data_dir / uci_filename)
        self.wonju_path = str(data_dir / wonju_filename)
        self.output_file = str(data_dir / "final_synthetic_ami_data.csv")
        
        self.num_users = num_users
        self.days = days
        self.total_minutes = days * 24 * 60

    def _load_and_process_wonju(self) -> np.ndarray:
        """
        원주시 1시간 단위 데이터를 읽어 '가구당 1분 평균 사용량 트렌드'로 변환 (Interpolation)
        """
        logging.info("원주시 거시적 트렌드(Macro Trend) 데이터를 분석합니다...")
        try:
            # 원주시 데이터 로드 (csv 파일의 컬럼명 기준)
            df_w = pd.read_csv(self.wonju_path, encoding='cp949') # 공공데이터는 주로 cp949 인코딩
            

            # 혹시 모를 컬럼명 양끝의 숨겨진 공백을 모두 제거
            df_w.columns = df_w.columns.str.strip()
            
            # 수정 전: df_w['per_user_hourly_kwh'] = df_w['전력사용량'] / df_w['고객호수']
            
            # 수정 후: 컬럼명을 정확히 맞추고, MWh를 kWh로 변환하기 위해 1000을 곱함.
            df_w['per_user_hourly_kwh'] = (df_w['전력사용량(MWh)'] * 1000) / df_w['고객호수']
            
            # 테스트할 날짜(30일 = 720시간)만큼 데이터 슬라이싱
            hourly_trend = df_w['per_user_hourly_kwh'].head(self.days * 24).values
            
            # 1시간(60분) 단위를 1분 단위로 부드럽게 늘림 (선형 보간법)
            minute_trend = np.repeat(hourly_trend, 60) / 60.0 
            return minute_trend.reshape(-1, 1)
        except Exception as e:
            logging.error(f"원주시 데이터 로드 실패. 파일명과 컬럼(전력사용량, 고객호수)을 확인하세요: {e}")
            raise

    def _load_and_process_uci(self) -> np.ndarray:
        """
        UCI 1분 단위 데이터를 읽어 '미세 패턴 비율(Micro Pattern Ratio)'로 정규화
        """
        logging.info("UCI 미세 시계열 패턴(Micro Pattern) 데이터를 분석합니다...")
        try:
            # 결측치(?) 처리 및 세미콜론 구분자 명시
            df_u = pd.read_csv(self.uci_path, sep=';', na_values=['?', ''], low_memory=False)
            
            # Global_active_power (kW 단위) 추출 및 결측치는 앞의 값으로 채움
            power_series = df_u['Global_active_power'].ffill().values
            
            # 필요한 기간(30일 = 43200분)만큼 슬라이싱
            power_slice = power_series[:self.total_minutes]
            
            # 평균이 1.0이 되도록 정규화 (스케일은 원주 데이터가 결정하므로, 여기선 비율만 추출)
            normalized_pattern = power_slice / np.mean(power_slice)
            return normalized_pattern.reshape(-1, 1)
        except Exception as e:
            logging.error(f"UCI 데이터 로드 실패. 파일명(uci_power.txt)을 확인하세요: {e}")
            raise

    def generate(self, chunk_size=100):
        logging.info(f"데이터 융합 시작: 총 {self.num_users}가구, {self.days}일치 생성")
        
        # 1. 두 리얼 데이터의 융합 (Macro * Micro)
        wonju_trend = self._load_and_process_wonju()
        uci_pattern = self._load_and_process_uci()
        
        # 원주시의 거시적 스케일에 UCI의 1분 단위 지글거림(분산)을 곱함
        fused_base_pattern = wonju_trend * uci_pattern
        
        timestamps = [datetime(2024, 8, 1) + pd.Timedelta(minutes=i) for i in range(self.total_minutes)]
        
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
            
        header_written = False
        
        # 2. 청크 단위로 가구별 특성 부여 및 저장
        for chunk_start in range(0, self.num_users, chunk_size):
            chunk_end = min(chunk_start + chunk_size, self.num_users)
            current_chunk = chunk_end - chunk_start
            
            # 가구별 전력 사용 성향 (0.7배 ~ 1.3배)
            user_multipliers = np.random.uniform(0.7, 1.3, size=(1, current_chunk))
            
            # 행렬 연산
            usage_matrix = fused_base_pattern * user_multipliers
            
            # 약간의 가구별 랜덤 노이즈 추가
            noise = np.random.normal(0, np.mean(fused_base_pattern) * 0.1, usage_matrix.shape)
            final_usage = np.clip(usage_matrix + noise, 0, None) # 음수 방지
            
            user_ids = [f"USER_{str(i).zfill(4)}" for i in range(chunk_start, chunk_end)]
            df_chunk = pd.DataFrame(final_usage, columns=user_ids)
            df_chunk['timestamp'] = timestamps
            
            # Unpivot 및 최적화
            df_long = df_chunk.melt(id_vars=['timestamp'], var_name='user_id', value_name='kwh_usage')
            df_long['user_id'] = df_long['user_id'].astype('category')
            df_long['kwh_usage'] = df_long['kwh_usage'].astype('float32')
            
            df_long.to_csv(self.output_file, mode='w' if not header_written else 'a', 
                           header=not header_written, index=False)
            header_written = True
            
            logging.info(f"진척도: {chunk_end}/{self.num_users} 가구 완료...")

        logging.info(f"데이터 생성 완료! [{self.output_file}]")

if __name__ == "__main__":
    np.random.seed(42)
    # 메모리 상황에 따라 num_users를 100 등 적게 설정하여 먼저 테스트.
    generator = DataFusionGenerator(num_users=1000, days=30)
    generator.generate(chunk_size=100)