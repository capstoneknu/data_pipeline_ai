import torch
import torch.nn as nn
import math

class KHNPSmartDRNet(nn.Module):
    def __init__(self, num_users=10000, user_embed_dim=16, feature_dim=5, hidden_dim=128, num_layers=3, pred_len=96):
        """
        [24h 확장 스펙 및 1만 가구 스케일 대응] 
        - num_users: 10000 (IndexError 방어)
        - pred_len: 96 (24시간 * 15분 단위 4개)
        - hidden_dim: 128
        - num_layers: 3
        """
        super(KHNPSmartDRNet, self).__init__()
        
        self.user_embedding = nn.Embedding(num_embeddings=num_users, embedding_dim=user_embed_dim)
        
        self.lstm = nn.LSTM(
            input_size=feature_dim, 
            hidden_size=hidden_dim, 
            num_layers=num_layers, 
            batch_first=True, 
            bidirectional=True
        )
        
        self.layer_norm1 = nn.LayerNorm(hidden_dim * 2)
        
        self.attention = nn.MultiheadAttention(
            embed_dim=hidden_dim * 2, 
            num_heads=8, 
            batch_first=True
        )
        
        self.layer_norm2 = nn.LayerNorm(hidden_dim * 2)
        
        self.fc = nn.Sequential(
            nn.Linear((hidden_dim * 2) + user_embed_dim, 256),
            nn.LayerNorm(256), 
            nn.GELU(),
            nn.Dropout(0.3), 
            nn.Linear(256, pred_len)
        )
        
    def forward(self, x, user_id):
        user_vec = self.user_embedding(user_id)
        
        lstm_out, _ = self.lstm(x)
        lstm_out = self.layer_norm1(lstm_out) 
        
        attn_out, _ = self.attention(lstm_out, lstm_out, lstm_out)
        attn_out = self.layer_norm2(attn_out + lstm_out) 
        
        context_vector = attn_out[:, -1, :] 
        
        combined_features = torch.cat((context_vector, user_vec), dim=1)
        predictions = self.fc(combined_features)
        
        return predictions

if __name__ == "__main__":
    # 10,000 유저 스케일 테스트로 수정
    dummy_x = torch.rand(128, 96, 5)     
    dummy_user = torch.randint(0, 10000, (128,)) 
    
    model = KHNPSmartDRNet()
    output = model(dummy_x, dummy_user)
    
    print(f"AI 모델 텐서 출력 성공! 형태: {output.shape}")