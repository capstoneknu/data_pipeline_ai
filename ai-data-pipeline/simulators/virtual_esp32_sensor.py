import pandas as pd
import json
import time
import logging
from confluent_kafka import Producer
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [ESP32-CLUSTER] - %(message)s')

class VirtualESP32Sensor:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.db_topic = 'power-usage-topic'       
        self.ws_topic = 'power-telemetry-topic'   
        
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'linger.ms': 10,                 
            'batch.size': 65536,             
            'queue.buffering.max.messages': 500000, 
            'compression.type': 'lz4'
        })

    def _delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"메시지 전송 실패: {err}")

    def start_streaming(self, target_tps=5000):
        logging.info(f"가상 ESP32 클러스터 가동! 10,000가구 트래픽 발사 시작 (목표: {target_tps} TPS)")
        
        count = 0
        start_time = time.time()
        
        try:
            chunk_iterator = pd.read_csv(self.csv_file, chunksize=100000)
            
            for chunk in chunk_iterator:                
                for row in chunk.itertuples(index=False):
                    uid_str = str(row.user_id)
                    kwh_val = float(row.kwh_usage)
                    
                    # 1. Raw 문자열 추출 
                    ts_raw = str(row.timestamp)
                    
                    # 2. Python datetime 객체로 파싱
                    dt_obj = pd.to_datetime(ts_raw).to_pydatetime()
                    if dt_obj.tzinfo is None:
                        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                    
                    # 3. ISO 8601 글로벌 표준 문자열로 재직렬화
                    strict_ts_iso = dt_obj.isoformat()
                    
                    # 4. WebSocket을 위한 Float 변환
                    ts_float = dt_obj.timestamp()

                    # 수정된 페이로드 (strict_ts_iso 사용)
                    sensor_data_db = {"device_id": uid_str, "timestamp": strict_ts_iso, "kwh_usage": kwh_val}
                    sensor_data_ws = {"user_id": uid_str, "timestamp": ts_float, "power_kwh": kwh_val}
                    
                    self.producer.produce(self.db_topic, key=uid_str, value=json.dumps(sensor_data_db).encode('utf-8'), callback=self._delivery_report)
                    self.producer.produce(self.ws_topic, key=uid_str, value=json.dumps(sensor_data_ws).encode('utf-8'), callback=self._delivery_report)
                    
                    count += 1
                    
                    if count % 10000 == 0:
                        self.producer.poll(0)
                        elapsed = time.time() - start_time
                        current_tps = count / elapsed
                        logging.info(f"⚡ {count:,}건 스트리밍 완료... 현재 속도: {current_tps:,.1f} TPS")
                        
                        if current_tps > target_tps:
                            sleep_time = (count / target_tps) - elapsed
                            if sleep_time > 0:
                                time.sleep(sleep_time)

        except FileNotFoundError:
            logging.error("데이터 파일이 없습니다. generate_dr_data.py를 먼저 실행하세요.")
        except KeyboardInterrupt:
            logging.info("관리자에 의해 스트리밍이 중단되었습니다.")
        finally:
            logging.info("Kafka 버퍼에 남은 잔여 메시지 플러시 중...")
            self.producer.flush()
            logging.info("가상 센서 송신 완전 종료.")

if __name__ == "__main__":
    TARGET_CSV = "../data/final_synthetic_ami_data.csv" 
    sensor = VirtualESP32Sensor(csv_file=TARGET_CSV)
    sensor.start_streaming(target_tps=10000)