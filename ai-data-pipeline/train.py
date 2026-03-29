import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import CosineAnnealingWarmRestarts
import logging
import os
import copy

# 이전 단계에서 만든 파일들 임포트
from data_loader import InfluxPowerDataLoader, PowerSlidingWindowDataset
from model import KHNPSmartDRNet
from torch.utils.data import DataLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class EarlyStopping:
    """논문 5. 모델이 노이즈를 암기하기 전에 학습을 강제 종료하는 방어 기제"""
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
            logging.info(f"⚠️ Early Stopping 카운트: {self.counter} / {self.patience}")
            if self.counter >= self.patience:
                self.early_stop = True

def train_model():
    # 1. 하드웨어 세팅 (GPU가 있으면 자동 할당)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"🖥️ 학습 장치 할당: {device}")

    # 2. 데이터 준비 (이전 단계의 영양식)
    loader = InfluxPowerDataLoader()
    raw_df = loader.fetch_data_from_influx()
    train_df, val_df, feature_cols = loader.preprocess_and_split(raw_df)
    
    train_dataset = PowerSlidingWindowDataset(train_df, feature_cols, seq_length=16, pred_length=4)
    val_dataset = PowerSlidingWindowDataset(val_df, feature_cols, seq_length=16, pred_length=4)
    
    train_loader = DataLoader(train_dataset, batch_size=256, shuffle=True, drop_last=True)
    val_loader = DataLoader(val_dataset, batch_size=256, shuffle=False)

    # 3. 뇌 구조(Model) 및 논문 기반 최적화 도구 탑재
    model = KHNPSmartDRNet().to(device)
    
    # [논문 4] Huber Loss (이상치 스파이크 방어)
    criterion = nn.HuberLoss(delta=1.0) 
    
    # [논문 1] AdamW Optimizer (과적합 방지)
    optimizer = optim.AdamW(model.parameters(), lr=0.001, weight_decay=1e-4)
    
    # [논문 2] Cosine Annealing Scheduler (지역 최적해 탈출)
    scheduler = CosineAnnealingWarmRestarts(optimizer, T_0=10, T_mult=2)
    
    # [논문 5] Early Stopping
    early_stopping = EarlyStopping(patience=5, delta=0.0005)

    num_epochs = 100
    logging.info("🔥 심장 구동(Training Loop)을 시작합니다!")

    for epoch in range(num_epochs):
        # --- Training Phase ---
        model.train()
        train_loss = 0.0
        
        for x_batch, user_batch, y_batch in train_loader:
            x_batch, user_batch, y_batch = x_batch.to(device), user_batch.to(device), y_batch.to(device)
            
            optimizer.zero_grad()
            predictions = model(x_batch, user_batch)
            loss = criterion(predictions, y_batch)
            loss.backward()
            
            # [논문 3] Gradient Clipping (LSTM 기울기 폭발 방어)
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            
            optimizer.step()
            train_loss += loss.item() * x_batch.size(0)
            
        train_loss /= len(train_loader.dataset)
        
        # --- Validation Phase ---
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x_batch, user_batch, y_batch in val_loader:
                x_batch, user_batch, y_batch = x_batch.to(device), user_batch.to(device), y_batch.to(device)
                predictions = model(x_batch, user_batch)
                loss = criterion(predictions, y_batch)
                val_loss += loss.item() * x_batch.size(0)
                
        val_loss /= len(val_loader.dataset)
        scheduler.step() # 코사인 스케줄러 업데이트
        
        logging.info(f"Epoch [{epoch+1}/{num_epochs}] | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f} | LR: {scheduler.get_last_lr()[0]:.6f}")
        
        # Early Stopping 체크
        early_stopping(val_loss, model)
        if early_stopping.early_stop:
            logging.info(f"🛑 검증 오차가 더 이상 줄어들지 않아 학습을 조기 종료합니다. (Epoch {epoch+1})")
            model.load_state_dict(early_stopping.best_model_weights) # 가장 똑똑했던 상태로 복원
            break

    # 4. 가장 똑똑하게 학습된 뇌(Weights)를 저장
    os.makedirs('saved_models', exist_ok=True)
    torch.save(model.state_dict(), '../saved_models/khnp_dr_best_model.pth')
    logging.info("💾 AI의 뇌 구조가 [khnp_dr_best_model.pth] 파일로 안전하게 저장되었습니다!")

if __name__ == "__main__":
    train_model()