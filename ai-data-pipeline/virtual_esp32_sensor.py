import pandas as pd
import json
import time
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class VirtualESP32Sensor:
    def __init__(self, csv_file, topic_name='power-usage-topic'):
        self.csv_file = csv_file
        self.topic_name = topic_name
        # 카프카 브로커 연결 설정 (도커 컨테이너)
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def delivery_report(self, err, msg):
        """메시지 전송 성공/실패 콜백 함수"""
        if err is not None:
            logging.error(f"메시지 전송 실패: {err}")

    def start_streaming(self, speed_ms=0.01):
        """CSV 데이터를 읽어서 실시간 스트리밍 모사"""
        logging.info(f"가상 ESP32 센서 가동! [{self.csv_file}] 데이터를 카프카로 쏩니다.")
        
        try:
            # 메모리 폭발 방지를 위해 chunksize로 끊어서 읽기
            chunk_iter = pd.read_csv(self.csv_file, chunksize=1000)
            
            count = 0
            for chunk in chunk_iter:
                for _, row in chunk.iterrows():
                    # 센서가 보내는 JSON 포맷으로 변환
                    sensor_data = {
                        "device_id": row['user_id'], # user_id를 기기 ID로 취급
                        "timestamp": str(row['timestamp']),
                        "kwh_usage": float(row['kwh_usage'])
                    }
                    
                    # 카프카로 비동기 전송 (Publish)
                    self.producer.produce(
                        self.topic_name, 
                        key=sensor_data['device_id'], # 파티셔닝 키
                        value=json.dumps(sensor_data).encode('utf-8'), 
                        callback=self.delivery_report
                    )
                    
                    count += 1
                    # 1000건마다 큐에 쌓인 메시지 비우기 및 로그 출력
                    if count % 1000 == 0:
                        self.producer.poll(0)
                        logging.info(f"⚡ {count}건 전송 완료...")
                    
                    # 진짜 센서처럼 미세한 딜레이 (데이터 쏟아짐 방지)
                    time.sleep(speed_ms)
                    
        except KeyboardInterrupt:
            logging.info("스트리밍을 사용자가 중단했습니다.")
        finally:
            # 남은 메시지 모두 밀어내기
            self.producer.flush()
            logging.info("센서 송신 종료.")

if __name__ == "__main__":
    # generator 파일로 생성한 초거대 CSV 파일 이름
    TARGET_CSV = "../data/final_synthetic_ami_data.csv" 
    
    sensor = VirtualESP32Sensor(csv_file=TARGET_CSV)
    # speed_ms: 0.01초마다 1건씩 쏜다 (1초에 100건)
    sensor.start_streaming(speed_ms=0.01)