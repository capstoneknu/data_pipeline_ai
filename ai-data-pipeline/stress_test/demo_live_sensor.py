import time
import json
import logging
import math
import random
from datetime import datetime, timedelta, timezone

# MQTT(에지 통신 표준) 라이브러리 사용
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SmartMeterSimulator:
    def __init__(self, user_id="11111111-1111-1111-1111-111111111111"):
        self.user_id = user_id
        
        # MQTT 브로커(Mosquitto) 설정
        self.mqtt_broker = '127.0.0.1'
        self.mqtt_port = 1883
        self.topic = f"dr/sensor/{self.user_id}" # 워커의 dr/sensor/+ 와일드카드에 매핑됨

        # MQTT v2.0.0 호환성 규격 준수 
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2, 
            client_id=f"esp32_simulator_{self.user_id}",
            protocol=mqtt.MQTTv5
        )

        now = datetime.now(timezone.utc)
        self.current_time = now.replace(hour=6, minute=0, second=0, microsecond=0)

    def generate_realistic_power(self, current_dt):
        hour = current_dt.hour + (current_dt.minute / 60.0)
        base_load = 0.05 
        activity_curve = max(0, math.sin(math.pi * (hour - 6) / 18)) * 0.4 
        spike = 0.0
        if 18 <= hour <= 22 and random.random() < 0.15:
            spike = random.uniform(0.3, 0.8)
        noise = random.uniform(-0.02, 0.02)
        total_kwh = max(0.01, base_load + activity_curve + spike + noise)
        return round(total_kwh, 3)

    def start_demo(self):
        logging.info("[스마트 미터 시뮬레이터 가동] 에지 디바이스(ESP32) 모드 - MQTT 스트리밍을 시작합니다...")
        
        try:
            # MQTT 브로커 연결 및 백그라운드 네트워크 루프 시작
            self.client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
            self.client.loop_start()

            while True:
                self.current_time += timedelta(minutes=15)
                simulated_kwh = self.generate_realistic_power(self.current_time)
                
                # 워커(mqtt_ingestion_worker.py)가 기대하는 정확한 JSON 스키마로 동기화
                sensor_data = {
                    "user_id": self.user_id,
                    "timestamp": self.current_time.timestamp(), # InfluxDB 처리를 위한 float 타임스탬프
                    "power_kwh": simulated_kwh
                }
                
                # Kafka send 대신 MQTT publish 수행 (QoS 1 적용으로 유실 방지)
                payload = json.dumps(sensor_data)
                self.client.publish(self.topic, payload, qos=1)
                
                logging.info(f"⚡ [가상 시간: {self.current_time.strftime('%H:%M')}] 전력 소비: {simulated_kwh} kWh 전송")
                
                time.sleep(10)
                
        except KeyboardInterrupt:
            logging.info("시연 종료.")
        finally:
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    sensor = SmartMeterSimulator()
    sensor.start_demo()