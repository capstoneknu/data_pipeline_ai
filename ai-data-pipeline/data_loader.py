import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler
from influxdb_client import InfluxDBClient
import logging
import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InfluxPowerDataLoader:
    def __init__(self, url="http://localhost:8086", token="super-secret-capstone-token", org="khnp-dr", bucket="power-data"):
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=100000)
        self.bucket = bucket
        self.scaler = MinMaxScaler()
        # 가구 ID(String)를 정수형(Int)으로 변환하기 위한 딕셔너리
        self.user_to_idx = {}

    def fetch_data_from_influx(self):
        """1. DB단에서 15분 단위로 데이터를 압축(Downsampling)하여 가져옴"""
        logging.info("InfluxDB에서 15분 단위로 압축된 데이터를 추출합니다. (수 분 소요될 수 있음)...")
        query = f"""
        from(bucket: "{self.bucket}")
          |> range(start: 2024-08-01T00:00:00Z, stop: 2024-08-30T00:00:00Z)
          |> filter(fn: (r) => r["_measurement"] == "power_usage")
          |> filter(fn: (r) => r["_field"] == "kwh_usage")
          |> aggregateWindow(every: 15m, fn: mean, createEmpty: false) 
          |> yield(name: "mean")
        """
        # Flux 쿼리 실행 후 Pandas DataFrame으로 바로 변환
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)
        
        # 쿼리 결과가 리스트 형태로 올 수 있으므로 단일 DF로 병합
        if isinstance(df, list):
            df = pd.concat(df)
            
        df = df[['_time', 'device_id', '_value']].rename(columns={'_time': 'timestamp', '_value': 'kwh_usage'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by=['device_id', 'timestamp']).reset_index(drop=True)
        return df

    def preprocess_and_split(self, df, train_ratio=0.8):
        """2. 피처 엔지니어링 및 시간 분할 (Data Leakage 완벽 방어)"""
        logging.info("시간 주기성(Sin/Cos) 인코딩 및 Train/Val 분할을 시작합니다...")
        
        # 가구 ID를 0~999의 정수형 Index로 매핑 (Embedding 레이어용)
        unique_users = df['device_id'].unique()
        self.user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
        df['user_idx'] = df['device_id'].map(self.user_to_idx)

        # 시간 주기성 인코딩 (수학 공식 적용)
        # 하루 24시간, 1년 365일을 원의 좌표(Sin, Cos)로 변환하여 AI가 '연속성'을 이해하게 함
        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24.0)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24.0)
        df['day_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.dayofweek / 7.0)
        df['day_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.dayofweek / 7.0)

        # 시간 기반으로 쪼개기 (Train: 앞의 80% 기간, Val: 뒤의 20% 기간)
        split_time = df['timestamp'].quantile(train_ratio)
        train_df = df[df['timestamp'] < split_time].copy()
        val_df = df[df['timestamp'] >= split_time].copy()

        # [핵심] 스케일러는 오직 Train 데이터로만 Fit 수행
        train_df['kwh_scaled'] = self.scaler.fit_transform(train_df[['kwh_usage']])
        val_df['kwh_scaled'] = self.scaler.transform(val_df[['kwh_usage']])

        features = ['kwh_scaled', 'hour_sin', 'hour_cos', 'day_sin', 'day_cos']
        return train_df, val_df, features

# 3. [최적화 핵심] 메모리를 잡아먹지 않는 효율적인 Dataset 클래스
class PowerSlidingWindowDataset(Dataset):
    def __init__(self, df, feature_cols, seq_length=16, pred_length=4):
        """
        seq_length: 과거 데이터 길이 (예: 15분 단위 * 16개 = 과거 4시간)
        pred_length: 예측할 미래 길이 (예: 15분 단위 * 4개 = 미래 1시간)
        """
        self.seq_length = seq_length
        self.pred_length = pred_length
        
        # 연산 속도 극대화를 위해 Numpy/Tensor로 변환
        self.features = torch.tensor(df[feature_cols].values, dtype=torch.float32)
        self.targets = torch.tensor(df['kwh_scaled'].values, dtype=torch.float32)
        self.user_ids = torch.tensor(df['user_idx'].values, dtype=torch.long)
        
        # 가구(User)가 바뀌는 경계선에서는 윈도우를 생성하지 않기 위해 인덱스 계산
        self.valid_indices = []
        user_groups = df.groupby('user_idx').indices
        
        for user, indices in user_groups.items():
            start_idx = indices[0]
            end_idx = indices[-1]
            # 한 가구의 데이터 안에서 온전한 (과거+미래) 세트가 나오는 시작점들만 기록
            max_valid_start = end_idx - (seq_length + pred_length) + 1
            if max_valid_start > start_idx:
                self.valid_indices.extend(range(start_idx, max_valid_start))

    def __len__(self):
        return len(self.valid_indices)

    def __getitem__(self, idx):
        # 배열을 복제하지 않고 '뷰(View)'만 제공하여 메모리 $O(1)$ 유지
        start = self.valid_indices[idx]
        seq_end = start + self.seq_length
        pred_end = seq_end + self.pred_length
        
        # X: [seq_length, features], Y: [pred_length], User: [1]
        x = self.features[start:seq_end]
        y = self.targets[seq_end:pred_end]
        user_id = self.user_ids[start] 
        
        return x, user_id, y

if __name__ == "__main__":
    loader = InfluxPowerDataLoader()
    
    # 1. 데이터 로드 (DB에서 압축해서 가져옴)
    raw_df = loader.fetch_data_from_influx()
    logging.info(f"추출 완료! 총 데이터 크기: {len(raw_df)} 행")
    
    # 2. 전처리 및 분할
    train_df, val_df, feature_cols = loader.preprocess_and_split(raw_df)
    
    # 3. 효율적인 PyTorch Dataset 생성 (과거 4시간 보고 미래 1시간 예측)
    train_dataset = PowerSlidingWindowDataset(train_df, feature_cols, seq_length=16, pred_length=4)
    train_loader = DataLoader(train_dataset, batch_size=256, shuffle=True, num_workers=0)
    
    # 4. 검증 출력 (AI 입에 들어갈 숟가락 모양 확인)
    x_batch, user_batch, y_batch = next(iter(train_loader))
    logging.info(f"입력 데이터 X 형태 (Batch, Seq_len, Features): {x_batch.shape}")
    logging.info(f"유저 ID 임베딩 형태 (Batch): {user_batch.shape}")
    logging.info(f"정답 레이블 Y 형태 (Batch, Pred_len): {y_batch.shape}")