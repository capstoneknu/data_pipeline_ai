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
        self.user_to_idx = {}

    def fetch_data_from_influx(self):
        logging.info("InfluxDB에서 15분 단위로 압축된 데이터를 추출합니다...")
        query = f"""
        from(bucket: "{self.bucket}")
          |> range(start: 0, stop: 2030-01-01T00:00:00Z) 
          |> filter(fn: (r) => r["_measurement"] == "power_usage")
          |> filter(fn: (r) => r["_field"] == "kwh_usage")
          |> aggregateWindow(every: 15m, fn: mean, createEmpty: false) 
          |> yield(name: "mean")
        """
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)
        
        if isinstance(df, list):
            if len(df) == 0:
                raise ValueError("DB가 완전히 텅 비어 있습니다!")
            df = pd.concat(df)

        if df.empty:
            raise ValueError("DB가 완전히 텅 비어 있습니다!")
            
        df = df[['_time', 'device_id', '_value']].rename(columns={'_time': 'timestamp', '_value': 'kwh_usage'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by=['device_id', 'timestamp']).reset_index(drop=True)
        return df

    def preprocess_and_split(self, df, train_ratio=0.8):
        logging.info("피처 엔지니어링(시간 코사인 임베딩) 및 Train/Val 분할...")
        unique_users = df['device_id'].unique()
        self.user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
        df['user_idx'] = df['device_id'].map(self.user_to_idx)

        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24.0)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24.0)
        df['day_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.dayofweek / 7.0)
        df['day_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.dayofweek / 7.0)

        split_time = df['timestamp'].quantile(train_ratio)
        train_df = df[df['timestamp'] < split_time].copy()
        val_df = df[df['timestamp'] >= split_time].copy()

        train_df['kwh_scaled'] = self.scaler.fit_transform(train_df[['kwh_usage']])
        val_df['kwh_scaled'] = self.scaler.transform(val_df[['kwh_usage']])

        features = ['kwh_scaled', 'hour_sin', 'hour_cos', 'day_sin', 'day_cos']
        return train_df, val_df, features

class PowerSlidingWindowDataset(Dataset):
    def __init__(self, df, feature_cols, seq_length=96, pred_length=96):
        """[24h 스펙 동기화] 과거 24시간(96) -> 미래 24시간(96) 세팅"""
        self.seq_length = seq_length
        self.pred_length = pred_length
        
        self.features = torch.tensor(df[feature_cols].values, dtype=torch.float32)
        self.targets = torch.tensor(df['kwh_scaled'].values, dtype=torch.float32)
        self.user_ids = torch.tensor(df['user_idx'].values, dtype=torch.long)
        
        self.valid_indices = []
        user_groups = df.groupby('user_idx').indices
        
        for user, indices in user_groups.items():
            start_idx = indices[0]
            end_idx = indices[-1]
            max_valid_start = end_idx - (seq_length + pred_length) + 1
            if max_valid_start > start_idx:
                self.valid_indices.extend(range(start_idx, max_valid_start))

    def __len__(self):
        return len(self.valid_indices)

    def __getitem__(self, idx):
        start = self.valid_indices[idx]
        seq_end = start + self.seq_length
        pred_end = seq_end + self.pred_length
        
        x = self.features[start:seq_end]
        y = self.targets[seq_end:pred_end]
        user_id = self.user_ids[start] 
        
        return x, user_id, y

if __name__ == "__main__":
    loader = InfluxPowerDataLoader()
    raw_df = loader.fetch_data_from_influx()
    train_df, val_df, feature_cols = loader.preprocess_and_split(raw_df)
    
    # 96-in 96-out 검증
    train_dataset = PowerSlidingWindowDataset(train_df, feature_cols, seq_length=96, pred_length=96)
    train_loader = DataLoader(train_dataset, batch_size=128, shuffle=True)
    
    x_batch, user_batch, y_batch = next(iter(train_loader))
    print(f"X: {x_batch.shape}, Y: {y_batch.shape}")