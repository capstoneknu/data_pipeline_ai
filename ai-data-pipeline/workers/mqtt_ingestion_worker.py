import os
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MQTT-BRIDGE] - %(levelname)s - %(message)s')

class MqttToKafkaBridgeWorker:
    def __init__(self):
        self.mqtt_broker = os.getenv('MQTT_BROKER_HOST', '127.0.0.1')
        self.mqtt_port = int(os.getenv('MQTT_BROKER_PORT', 1883))
        self.kafka_server = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
        
        # Java 서버와 동일한 토픽명으로 동기화
        self.kafka_topic = 'power-usage-topic' 

        # confluent_kafka Producer 최적화 설정
        self.kafka_producer = Producer({
            'bootstrap.servers': self.kafka_server,
            'linger.ms': 5,
            'compression.type': 'lz4',
            'queue.buffering.max.messages': 100000
        })

        self.executor = ThreadPoolExecutor(max_workers=10)
        unique_client_id = f"dr_mqtt_bridge_{uuid.uuid4().hex[:8]}"

        self.mqtt_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=unique_client_id, 
            protocol=mqtt.MQTTv5
        )
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logging.info("MQTT 브로커 연결 성공. 센서 데이터 구독 시작...")
            client.subscribe("dr/sensor/+", qos=1) 
        else:
            logging.error(f"MQTT 브로커 연결 실패: {reason_code}")

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        logging.warning(f"MQTT 브로커 연결 해제. (Reason: {reason_code})")

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode('utf-8')
        self.executor.submit(self._process_payload, payload)

    def _delivery_report(self, err, msg):
        """Kafka 비동기 전송 성공/실패 콜백"""
        if err is not None:
            logging.error(f"Kafka 전송 실패: {err}")

    def _process_payload(self, payload):
        try:
            data = json.loads(payload)
            # 센서 원본 데이터 추출 (키 이름이 파편화되어 있을 수 있으므로 유연하게 파싱)
            uid = data.get('user_id') or data.get('device_id')
            pwr = float(data.get('power_kwh') or data.get('kwh_usage') or 0.0)
            
            if not uid:
                raise ValueError("Payload에 식별자(user_id/device_id) 누락")

            # B파트(Java) Contract 준수를 위한 페이로드 재조립
            # Java: String deviceId, String timestampStr(yyyy-MM-dd HH:mm:ss), double kwhUsage
            
            # 수정 전 (mqtt_ingestion_worker.py)
            # iso_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # 수정 후 (ISO 8601 표준 직렬화)
            iso_time = datetime.now(timezone.utc).isoformat()

            kafka_payload = {
                "device_id": str(uid),
                "timestamp": iso_time,
                "kwh_usage": pwr
            }

            # ========================================================
            # [추가] 물리 센서 인입 확인용 Traceability 로그
            # ========================================================
            logging.info(f"[PHYSICAL INGESTION] Device: {uid}, Power: {pwr} kW")

            # Kafka 전송 (직렬화 수행)
            self.kafka_producer.produce(
                self.kafka_topic,
                key=str(uid).encode('utf-8'),
                value=json.dumps(kafka_payload).encode('utf-8'),
                callback=self._delivery_report
            )
            # 메모리 버퍼 폭주를 막기 위한 비동기 poll 호출
            self.kafka_producer.poll(0)

        except json.JSONDecodeError:
            logging.error("잘못된 JSON 형식 수신")
        except Exception as e:
            logging.error(f"데이터 브릿지 처리 중 에러: {e}")

    def run(self):
        logging.info("A파트 MQTT ➔ Kafka 브릿지 워커 기동...")
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            logging.info("워커 안전 종료 중...")
        finally:
            self.mqtt_client.disconnect()
            self.kafka_producer.flush() # 남은 Kafka 버퍼 강제 전송
            self.executor.shutdown(wait=True)

if __name__ == "__main__":
    worker = MqttToKafkaBridgeWorker()
    worker.run()