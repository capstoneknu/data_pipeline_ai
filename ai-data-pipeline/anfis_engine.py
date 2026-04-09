import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DREnergyFuzzyEngine:
    """
    AI(LSTM)의 예측 전력량(CBL)을 바탕으로, 
    유저 신뢰도와 전력망 스트레스를 고려해 맞춤형 미션(목표치, 보상)을 산출하는 퍼지 논리 엔진
    """
    def __init__(self):
        self._build_fuzzy_system()

    def _build_fuzzy_system(self):
        # 1. 입력 변수 (Antecedents) 정의 (0.0 ~ 1.0)
        self.user_reliability = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'user_reliability')
        self.grid_stress = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'grid_stress')

        # 2. 출력 변수 (Consequents) 정의
        # 감축 비율 (5% ~ 30%)
        self.curtail_ratio = ctrl.Consequent(np.arange(0.05, 0.31, 0.01), 'curtail_ratio')
        # 보상 배수 (1.0x ~ 3.0x)
        self.reward_multiplier = ctrl.Consequent(np.arange(1.0, 3.1, 0.1), 'reward_multiplier')

        # 3. 멤버십 함수 (Membership Functions) 자동 할당 (Low, Average, Good 등)
        self.user_reliability.automf(3, names=['low', 'medium', 'high'])
        self.grid_stress.automf(3, names=['normal', 'warning', 'emergency'])

        # 출력 변수 멤버십 함수 수동 설계 (세밀한 제어를 위해)
        self.curtail_ratio['low'] = fuzz.trimf(self.curtail_ratio.universe, [0.05, 0.05, 0.15])
        self.curtail_ratio['medium'] = fuzz.trimf(self.curtail_ratio.universe, [0.10, 0.15, 0.25])
        self.curtail_ratio['high'] = fuzz.trimf(self.curtail_ratio.universe, [0.20, 0.30, 0.30])

        self.reward_multiplier['low'] = fuzz.trimf(self.reward_multiplier.universe, [1.0, 1.0, 2.0])
        self.reward_multiplier['medium'] = fuzz.trimf(self.reward_multiplier.universe, [1.5, 2.0, 2.5])
        self.reward_multiplier['high'] = fuzz.trimf(self.reward_multiplier.universe, [2.0, 3.0, 3.0])

        # 4. 퍼지 룰 베이스 (Rule Base) - 총 9개 매트릭스 완벽 대응
        rules = [
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['normal'], 
                      (self.curtail_ratio['low'], self.reward_multiplier['low'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['normal'], 
                      (self.curtail_ratio['low'], self.reward_multiplier['low'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['normal'], 
                      (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['warning'], 
                      (self.curtail_ratio['low'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['warning'], 
                      (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['warning'], 
                      (self.curtail_ratio['high'], self.reward_multiplier['high'])),
            
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['emergency'], 
                      (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['emergency'], 
                      (self.curtail_ratio['high'], self.reward_multiplier['high'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['emergency'], 
                      (self.curtail_ratio['high'], self.reward_multiplier['high']))
        ]

        # 5. 제어 시스템 시뮬레이터 구축
        self.dr_ctrl = ctrl.ControlSystem(rules)
        self.dr_sim = ctrl.ControlSystemSimulation(self.dr_ctrl)

    def generate_mission(self, user_id: str, predicted_cbl: float, reliability: float, stress: float, base_points: int = 500) -> dict:
        """
        AI 예측값과 상황 변수를 입력받아 최종 미션 JSON 명세를 산출
        """
        try:
            # 입력값 캡핑 (안전장치)
            self.dr_sim.input['user_reliability'] = max(0.0, min(1.0, reliability))
            self.dr_sim.input['grid_stress'] = max(0.0, min(1.0, stress))

            # 퍼지 연산 (Centroid 디퍼지화)
            self.dr_sim.compute()

            # 결과 추출
            out_ratio = self.dr_sim.output['curtail_ratio']
            out_mult = self.dr_sim.output['reward_multiplier']

            # 최종 비즈니스 로직 산출
            target_kwh = predicted_cbl * (1 - out_ratio)
            final_points = int(base_points * out_mult)
            
            # 난이도 라벨링
            if out_ratio >= 0.20: difficulty = "Hard"
            elif out_ratio >= 0.12: difficulty = "Medium"
            else: difficulty = "Easy"

            result = {
                "user_id": user_id,
                "predicted_cbl_kwh": float(round(predicted_cbl, 2)), # float 씌우기
                "mission_target_kwh": float(round(target_kwh, 2)),   # float 씌우기
                "curtailment_ratio_percent": float(round(out_ratio * 100, 1)), # float 씌우기
                "expected_reward_points": int(final_points),         # int 씌우기
                "difficulty": difficulty
            }
            return result

        except Exception as e:
            logging.error(f"Fuzzy Engine Error for {user_id}: {e}")
            # 에러 발생 시 최하 난이도로 안전하게 Fallback
            return {
                "user_id": user_id,
                "predicted_cbl_kwh": round(predicted_cbl, 2),
                "mission_target_kwh": round(predicted_cbl * 0.95, 2), # 5% 감축
                "curtailment_ratio_percent": 5.0,
                "expected_reward_points": base_points,
                "difficulty": "Easy"
            }

if __name__ == "__main__":
    # 전기 충격 테스트 (데이터가 잘 나오는지 확인)
    engine = DREnergyFuzzyEngine()
    
    # 상황 1: 평범한 신규 유저, 평온한 날씨
    print("--- Case 1: 신규 유저 & 평시 ---")
    mission1 = engine.generate_mission("USER_0001", predicted_cbl=10.5, reliability=0.5, stress=0.1)
    print(mission1)

    # 상황 2: 우수 참여 유저, 전력 비상(폭염)
    print("\n--- Case 2: 우수 유저 & 폭염 위기 ---")
    mission2 = engine.generate_mission("USER_0042", predicted_cbl=12.0, reliability=0.9, stress=0.9)
    print(mission2)