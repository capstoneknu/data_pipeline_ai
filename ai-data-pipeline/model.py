import torch
import torch.nn as nn
import math
#AI 뇌 구조(아키텍처) 설계
class KHNPSmartDRNet(nn.Module):
    def __init__(self, num_users=1000, user_embed_dim=16, feature_dim=5, hidden_dim=64, num_layers=2, pred_len=4):
        super(KHNPSmartDRNet, self).__init__()
        
        # 1. User Embedding Layer: 가구별 고유한 전력 소비 DNA를 학습
        # 단순한 ID(문자열)를 16차원의 실수 벡터 공간으로 매핑하여 가구 간의 '유사성'을 AI가 이해하게 함
        self.user_embedding = nn.Embedding(num_embeddings=num_users, embedding_dim=user_embed_dim)
        
        # 2. Bi-LSTM Layer: 과거와 미래 방향을 모두 스캔하여 시계열 문맥 추출
        self.lstm = nn.LSTM(
            input_size=feature_dim, 
            hidden_size=hidden_dim, 
            num_layers=num_layers, 
            batch_first=True, 
            bidirectional=True  # 🔥 [최적화] 양방향 스캔
        )
        
        # 3. Multi-Head Attention Layer: 어느 시간대의 데이터가 가장 중요한지 스스로 가중치 부여
        # Bi-LSTM이므로 hidden_dim * 2가 실제 차원이 됨
        self.attention = nn.MultiheadAttention(
            embed_dim=hidden_dim * 2, 
            num_heads=4, 
            batch_first=True
        )
        
        # 4. Fully Connected Layer (Regressor): 최종 미래 4스텝(1시간) 예측값 산출
        self.fc = nn.Sequential(
            nn.Linear((hidden_dim * 2) + user_embed_dim, 128),
            nn.GELU(),         # 최신 딥러닝 표준 활성화 함수 (ReLU보다 부드러운 기울기)
            nn.Dropout(0.2),   # 과적합(Overfitting) 방지
            nn.Linear(128, pred_len)
        )
        
    def forward(self, x, user_id):
        """
        x shape: [Batch, Seq_Len=16, Features=5]
        user_id shape: [Batch]
        """
        # Step 1. 가구 고유 DNA 추출 -> [Batch, User_Embed_Dim]
        user_vec = self.user_embedding(user_id)
        
        # Step 2. 시계열 패턴 양방향 추출 -> [Batch, Seq_Len, Hidden_Dim * 2]
        lstm_out, _ = self.lstm(x)
        
        # Step 3. Self-Attention으로 핵심 시간대에 집중
        # 쿼리(Q), 키(K), 값(V)으로 모두 lstm_out을 사용하여 자기 자신 안에서 중요도를 찾음
        attn_out, _ = self.attention(lstm_out, lstm_out, lstm_out)
        
        # Attention의 마지막 타임스텝 결과만 사용 (과거 문맥이 가장 고도로 압축된 상태)
        # shape: [Batch, Hidden_Dim * 2]
        context_vector = attn_out[:, -1, :] 
        
        # Step 4. 시계열 문맥(상황)과 가구 DNA(본성)를 하나로 융합
        # shape: [Batch, (Hidden_Dim * 2) + User_Embed_Dim]
        combined_features = torch.cat((context_vector, user_vec), dim=1)
        
        # Step 5. 미래 수요 예측값 발사 -> shape: [Batch, Pred_Len=4]
        predictions = self.fc(combined_features)
        
        return predictions

if __name__ == "__main__":
    # 뇌 구조가 정상 작동하는지 더미(Dummy) 데이터로 전기 충격 테스트
    dummy_x = torch.rand(256, 16, 5)     # 아까 뽑아낸 X 모양과 동일
    dummy_user = torch.randint(0, 1000, (256,)) # 0~999 가구 ID
    
    model = KHNPSmartDRNet()
    output = model(dummy_x, dummy_user)
    
    print(f"🔥 AI 모델 텐서 출력 성공! 형태: {output.shape}") 
    # torch.Size([256, 4]) 가 나오면 완벽하게 성공입니다!