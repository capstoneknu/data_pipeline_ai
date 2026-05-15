import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WriteOptions
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [INFLUX-INGESTION] - %(levelname)s - %(message)s')

class InfluxIngestionWorker:
    def __init__(self):
        self.topic = 'power-usage-topic'
        
        # confluent_kafka Consumer 최적화
        # Java(Spring)와 다른 group.id를 사용하여 데이터를 복제(Pub/Sub)받음
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'python-influx-ingestion-group', 
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False # 수동 커밋을 통한 데이터 유실 방어
        })
        self.consumer.subscribe([self.topic])

        # InfluxDB Client 설정
        self.influx_url = "http://localhost:8086"
        self.influx_token = "super-secret-capstone-token"
        self.influx_org = "khnp-dr"
        self.influx_bucket = "power-data"
        
        self.client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        
        # 비동기 배치 쓰기 최적화 (500개씩 모아서 전송)
        self.write_api = self.client.write_api(write_options=WriteOptions(
            batch_size=500, 
            flush_interval=1000,
            jitter_interval=200,
            retry_interval=5000
        ))
        
    def _parse_timestamp(self, ts_str):
        return parser.parse(ts_str)

    def start_consuming(self):
        logging.info("InfluxDB 적재 워커 가동! Kafka 데이터를 시계열로 영속화합니다...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Kafka Consumer Error: {msg.error()}")
                        break

                # 정상 메시지 처리
                try:
                    payload = msg.value().decode('utf-8')
                    data = json.loads(payload)

                    # InfluxDB Point 생성 (Java Contract 데이터 스키마 파싱)
                    point = Point("power_usage") \
                        .tag("device_id", data['device_id']) \
                        .field("kwh_usage", float(data['kwh_usage'])) \
                        .time(self._parse_timestamp(data['timestamp']))

                    # 버퍼에 기록
                    self.write_api.write(bucket=self.influx_bucket, record=point)
                    
                    # 안전한 수동 커밋
                    self.consumer.commit(asynchronous=True)
                    
                except json.JSONDecodeError:
                    logging.error("잘못된 JSON 형식 수신")
                except KeyError as e:
                    logging.error(f"필수 Key 누락 에러: {e}")

        except KeyboardInterrupt:
            logging.info("적재 워커를 안전하게 종료합니다 (Graceful Shutdown)...")
        finally:
            self.write_api.flush() # 버퍼 강제 전송
            self.client.close()
            self.consumer.close()
            logging.info("모든 데이터베이스 및 큐 연결이 종료되었습니다.")

if __name__ == "__main__":
    worker = InfluxIngestionWorker()
    worker.start_consuming()