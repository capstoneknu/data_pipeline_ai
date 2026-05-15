import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import CosineAnnealingWarmRestarts
import logging
import os
import copy
import joblib

from data_loader import InfluxPowerDataLoader, PowerSlidingWindowDataset
from model import KHNPSmartDRNet
from torch.utils.data import DataLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class EarlyStopping:
    def __init__(self, patience=7, delta=0.001):
        self.patience = patience
        self.delta = delta
        self.counter = 0
        self.best_loss = float('inf')
        self.best_model_weights = None
        self.early_stop = False

    def __call__(self, val_loss, model):
        if val_loss < self.best_loss - self.delta:
            self.best_loss = val_loss
            self.best_model_weights = copy.deepcopy(model.state_dict())
            self.counter = 0
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.early_stop = True

def train_model():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"학습 장치 할당: {device}")

    loader = InfluxPowerDataLoader()
    raw_df = loader.fetch_data_from_influx()
    train_df, val_df, feature_cols = loader.preprocess_and_split(raw_df)  
    actual_num_users = len(loader.user_to_idx)
    logging.info(f"DB 내 인식된 총 가구 수: {actual_num_users} 가구")

    # [스펙 동기화] 96시간 예측 세팅
    train_dataset = PowerSlidingWindowDataset(train_df, feature_cols, seq_length=96, pred_length=96)
    val_dataset = PowerSlidingWindowDataset(val_df, feature_cols, seq_length=96, pred_length=96)
    
    # [OOM(out of memory) 방어 및 I/O 최적화] batch_size 128로 감축, pin_memory=True 적용
    train_loader = DataLoader(train_dataset, batch_size=128, shuffle=True, drop_last=True)
    val_loader = DataLoader(val_dataset, batch_size=128, shuffle=False)

    # 동적 아키텍처 적용
    model = KHNPSmartDRNet(num_users=actual_num_users).to(device)   

    criterion = nn.HuberLoss(delta=1.0) #
    optimizer = optim.AdamW(model.parameters(), lr=0.0005, weight_decay=1e-4) #[cite: 4]
    scheduler = CosineAnnealingWarmRestarts(optimizer, T_0=10, T_mult=2) #
    early_stopping = EarlyStopping(patience=5, delta=0.0005)

    num_epochs = 100
    logging.info("심장 구동(Training Loop)을 시작")

    for epoch in range(num_epochs):
        model.train()
        train_loss = 0.0
        
        for x_batch, user_batch, y_batch in train_loader:
            x_batch = x_batch.to(device, non_blocking=True)
            user_batch = user_batch.to(device, non_blocking=True)
            y_batch = y_batch.to(device, non_blocking=True)
            
            optimizer.zero_grad()
            predictions = model(x_batch, user_batch)
            loss = criterion(predictions, y_batch)
            loss.backward()
            
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            train_loss += loss.item() * x_batch.size(0)
            
        train_loss /= len(train_loader.dataset)
        
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x_batch, user_batch, y_batch in val_loader:
                x_batch = x_batch.to(device, non_blocking=True)
                user_batch = user_batch.to(device, non_blocking=True)
                y_batch = y_batch.to(device, non_blocking=True)
                
                predictions = model(x_batch, user_batch)
                loss = criterion(predictions, y_batch)
                val_loss += loss.item() * x_batch.size(0)
                
        val_loss /= len(val_loader.dataset)
        scheduler.step()
        
        logging.info(f"Epoch [{epoch+1}/{num_epochs}] | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f}")
        
        early_stopping(val_loss, model)
        if early_stopping.early_stop:
            logging.info(f"학습 조기 종료 (Epoch {epoch+1})")
            model.load_state_dict(early_stopping.best_model_weights)
            break

    os.makedirs('./saved_models', exist_ok=True)
    torch.save(model.state_dict(), './saved_models/khnp_dr_best_model.pth')
    joblib.dump(loader.scaler, './saved_models/scaler.pkl')
    joblib.dump(loader.user_to_idx, './saved_models/user_to_idx.pkl')
    logging.info("24시간 장기 예측 모델 (96-in/96-out) 저장 완료!")

if __name__ == "__main__":
    train_model()