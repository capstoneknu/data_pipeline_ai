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
        
        # 에지 필터링 통계용 변수
        self.filtered_outliers = 0

    def _delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"메시지 전송 실패: {err}")

    # O(1) 경량화 에지 이상치(Outlier) 탐지 필터
    def _edge_filter_outlier(self, kwh_val: float) -> bool:
        """ 
        물리적으로 불가능한 가정용 전력 데이터 폐기 
        - 음수 데이터 폐기
        - 1분 단위 최대 허용 전력량(약 15kW, 시간당 900kW 상당의 순간 스파이크) 초과분 폐기
        """
        if kwh_val < 0.0 or kwh_val > 15.0:
            self.filtered_outliers += 1
            return False
        return True

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
                    
                    # Edge Filtering 검증
                    if not self._edge_filter_outlier(kwh_val):
                        continue # 비정상 데이터는 Kafka 브로커로 전송하지 않고 폐기(Drop)
                    
                    ts_raw = str(row.timestamp)
                    
                    dt_obj = pd.to_datetime(ts_raw).to_pydatetime()
                    if dt_obj.tzinfo is None:
                        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                    
                    strict_ts_iso = dt_obj.isoformat()
                    ts_float = dt_obj.timestamp()

                    sensor_data_db = {"device_id": uid_str, "timestamp": strict_ts_iso, "kwh_usage": kwh_val}
                    sensor_data_ws = {"user_id": uid_str, "timestamp": ts_float, "power_kwh": kwh_val}
                    
                    self.producer.produce(self.db_topic, key=uid_str, value=json.dumps(sensor_data_db).encode('utf-8'), callback=self._delivery_report)
                    self.producer.produce(self.ws_topic, key=uid_str, value=json.dumps(sensor_data_ws).encode('utf-8'), callback=self._delivery_report)
                    
                    count += 1
                    
                    if count % 10000 == 0:
                        self.producer.poll(0)
                        elapsed = time.time() - start_time
                        current_tps = count / elapsed
                        logging.info(f"⚡ {count:,}건 스트리밍... 현재 속도: {current_tps:,.1f} TPS (필터링된 이상치: {self.filtered_outliers}건)")
                        
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
            logging.info(f"가상 센서 송신 완전 종료. (최종 필터링된 이상치 쓰레기 데이터: {self.filtered_outliers}건)")

if __name__ == "__main__":
    TARGET_CSV = "../data/final_synthetic_ami_data.csv" 
    sensor = VirtualESP32Sensor(csv_file=TARGET_CSV)
    sensor.start_streaming(target_tps=10000)