import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [ANFIS] - %(levelname)s - %(message)s')

class DREnergyFuzzyEngine:
    """
    [Thread-Safe ANFIS 엔진]
    AI 예측 전력량(CBL)을 바탕으로 맞춤형 미션을 산출하는 동적 난이도 조절(DDA) 퍼지 엔진.
    Stateless 구조로 설계하여, 1만 가구 병렬 처리 시 Race Condition을 차단함.
    """
    def __init__(self):
        self._build_fuzzy_system()

    def _build_fuzzy_system(self):
        # 1. 입력 변수 (Antecedents) - 0.0 ~ 1.0 정규화 스케일
        self.user_reliability = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'user_reliability')
        self.grid_stress = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'grid_stress')

        # 2. 출력 변수 (Consequents) 
        self.curtail_ratio = ctrl.Consequent(np.arange(0.05, 0.31, 0.01), 'curtail_ratio')
        self.reward_multiplier = ctrl.Consequent(np.arange(1.0, 3.1, 0.1), 'reward_multiplier')

        # 3. 가우시안 멤버십 함수 (급격한 단절을 막고 부드러운 난이도 곡선 제공)
        self.user_reliability['low'] = fuzz.gaussmf(self.user_reliability.universe, 0.0, 0.2)
        self.user_reliability['medium'] = fuzz.gaussmf(self.user_reliability.universe, 0.5, 0.2)
        self.user_reliability['high'] = fuzz.gaussmf(self.user_reliability.universe, 1.0, 0.2)

        self.grid_stress['normal'] = fuzz.gaussmf(self.grid_stress.universe, 0.0, 0.2)
        self.grid_stress['warning'] = fuzz.gaussmf(self.grid_stress.universe, 0.5, 0.2)
        self.grid_stress['emergency'] = fuzz.gaussmf(self.grid_stress.universe, 1.0, 0.2)

        self.curtail_ratio['low'] = fuzz.gaussmf(self.curtail_ratio.universe, 0.05, 0.04)
        self.curtail_ratio['medium'] = fuzz.gaussmf(self.curtail_ratio.universe, 0.15, 0.04)
        self.curtail_ratio['high'] = fuzz.gaussmf(self.curtail_ratio.universe, 0.30, 0.04)

        self.reward_multiplier['low'] = fuzz.gaussmf(self.reward_multiplier.universe, 1.0, 0.3)
        self.reward_multiplier['medium'] = fuzz.gaussmf(self.reward_multiplier.universe, 2.0, 0.3)
        self.reward_multiplier['high'] = fuzz.gaussmf(self.reward_multiplier.universe, 3.0, 0.3)

        # 4. 퍼지 규칙 매트릭스 (9-Rules)
        rules = [
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['normal'], (self.curtail_ratio['low'], self.reward_multiplier['low'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['normal'], (self.curtail_ratio['low'], self.reward_multiplier['low'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['normal'], (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['warning'], (self.curtail_ratio['low'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['warning'], (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['warning'], (self.curtail_ratio['high'], self.reward_multiplier['high'])),
            ctrl.Rule(self.user_reliability['low'] & self.grid_stress['emergency'], (self.curtail_ratio['medium'], self.reward_multiplier['medium'])),
            ctrl.Rule(self.user_reliability['medium'] & self.grid_stress['emergency'], (self.curtail_ratio['high'], self.reward_multiplier['high'])),
            ctrl.Rule(self.user_reliability['high'] & self.grid_stress['emergency'], (self.curtail_ratio['high'], self.reward_multiplier['high']))
        ]
        self.dr_ctrl = ctrl.ControlSystem(rules)

    # [추가] XAI(설명 가능한 AI) 로그 생성기
    def _generate_xai_log(self, reliability: float, stress: float, difficulty: str) -> str:
        """ 입력된 퍼지 변수를 바탕으로 인간이 해석 가능한 형태의 인공지능 추론 근거 산출 """
        if stress >= 0.7: stress_desc = "전력망 부하가 극심한 긴급(Emergency) 상태"
        elif stress >= 0.4: stress_desc = "전력망 주의(Warning) 상태"
        else: stress_desc = "전력망 안정(Normal) 상태"

        if reliability >= 0.7: rel_desc = "사용자의 과거 미션 달성률(신뢰도)이 매우 우수하여"
        elif reliability >= 0.4: rel_desc = "사용자의 과거 미션 달성률이 평균적이므로"
        else: rel_desc = "사용자의 미션 실패율이 높아 이탈 방지를 위해"

        return f"[XAI 추론결과] {stress_desc}이며, {rel_desc} 동적 난이도 '{difficulty}' 및 맞춤형 보상 배율을 산출함."

    def generate_mission(self, user_id: str, predicted_cbl: float, reliability: float, stress: float, base_points: int = 500) -> dict:
        try:
            # 병렬 요청(Concurrent Requests) 시 메모리 충돌을 막는 로컬 시뮬레이터 인스턴스 
            local_sim = ctrl.ControlSystemSimulation(self.dr_ctrl)

            local_sim.input['user_reliability'] = max(0.0, min(1.0, float(reliability)))
            local_sim.input['grid_stress'] = max(0.0, min(1.0, float(stress)))
            local_sim.compute()

            out_ratio = float(local_sim.output['curtail_ratio'])
            out_mult = float(local_sim.output['reward_multiplier'])

            target_kwh = max(0.0, predicted_cbl * (1.0 - out_ratio))
            final_points = int(base_points * out_mult)

            if out_ratio >= 0.20: difficulty = "Hard"
            elif out_ratio >= 0.12: difficulty = "Medium"
            else: difficulty = "Easy"

            # XAI 로그 부착
            xai_log = self._generate_xai_log(reliability, stress, difficulty)
            logging.info(f"사용자 {user_id} XAI 산출: {xai_log}")

            return {
                "user_id": user_id,
                "predicted_cbl_kwh": float(round(predicted_cbl, 2)),
                "mission_target_kwh": float(round(target_kwh, 2)),
                "curtailment_ratio_percent": float(round(out_ratio * 100, 1)),
                "expected_reward_points": final_points,
                "difficulty": difficulty,
                "explainability_log": xai_log
            }

        except Exception as e:
            logging.error(f"Fuzzy Engine Breakdown for {user_id}: {e}")
            return {
                "user_id": user_id,
                "predicted_cbl_kwh": float(round(predicted_cbl, 2)),
                "mission_target_kwh": float(round(max(0.0, predicted_cbl * 0.95), 2)),
                "curtailment_ratio_percent": 5.0,
                "expected_reward_points": base_points,
                "difficulty": "Easy",
                "explainability_log": "[XAI Fallback] 엔진 붕괴로 인한 최소 난이도(Easy) 자동 배정"
            }