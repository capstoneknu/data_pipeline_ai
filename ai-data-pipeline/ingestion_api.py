import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from influxdb_client import InfluxDBClient, Point, WriteOptions

# 🔥 [추가] 어떤 시간 포맷이든 다 해석해내는 마법의 파서
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IngestionAPI:
    def __init__(self):
        # 1. Kafka Consumer 설정
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'ingestion-api-group', # 컨슈머 그룹 (스케일 아웃을 위한 핵심)
            'auto.offset.reset': 'earliest',   # 처음부터 못 읽은 데이터를 다 가져옴
            'enable.auto.commit': False        # 데이터 유실 방지를 위해 수동 커밋 사용 (시니어의 킥)
        })
        self.topic = 'power-usage-topic'
        self.consumer.subscribe([self.topic])

        # 2. InfluxDB Client 설정
        self.influx_url = "http://localhost:8086"
        self.influx_token = "super-secret-capstone-token"
        self.influx_org = "khnp-dr"
        self.influx_bucket = "power-data"
        
        self.client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        
        # [최적화 핵심] 비동기 배치 쓰기 (데이터를 모아서 한 번에 쏨)
        self.write_api = self.client.write_api(write_options=WriteOptions(
            batch_size=500, 
            flush_interval=1000,
            jitter_interval=200,
            retry_interval=5000
        ))
    # 🔥 [수정 전]
    # def _parse_timestamp(self, ts_str):
    #    """CSV에서 온 문자열 타임스탬프를 InfluxDB가 이해하는 포맷으로 변환"""
    #    # 예: '2024-08-01 00:00:00'
    #    return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    
    # 🔥 [수정 후] 
    def _parse_timestamp(self, ts_str):
        """초가 있든 없든, 유연하게 알아서 표준 datetime으로 파싱합니다."""
        return parser.parse(ts_str)

    def start_consuming(self):
        logging.info("🚀 Ingestion API 가동! Kafka에서 데이터를 꺼내어 InfluxDB로 적재합니다...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # 1. Kafka에서 JSON 데이터 추출
                raw_data = msg.value().decode('utf-8')
                data = json.loads(raw_data)

                # 2. InfluxDB 데이터 모델(Point)로 매핑
                # Measurement: 측정 주제 / Tag: 인덱싱 키 (기기 번호) / Field: 실제 값
                point = Point("power_usage") \
                    .tag("device_id", data['device_id']) \
                    .field("kwh_usage", float(data['kwh_usage'])) \
                    .time(self._parse_timestamp(data['timestamp']))

                # 3. 버퍼에 쓰기 (batch_size가 차면 자동으로 DB에 전송됨)
                self.write_api.write(bucket=self.influx_bucket, record=point)
                
                # 4. 처리 완료 후 오프셋 수동 커밋 (데이터 유실 방지)
                self.consumer.commit(asynchronous=True)

        except KeyboardInterrupt:
            logging.info("Ingestion API를 안전하게 종료합니다 (Graceful Shutdown)...")
        except Exception as e:
            logging.error(f"예기치 않은 에러 발생: {e}")
        finally:
            self.write_api.flush() # 버퍼에 남은 데이터 강제 전송
            self.client.close()
            self.consumer.close()
            logging.info("모든 연결이 안전하게 종료되었습니다.")

if __name__ == "__main__":
    api_worker = IngestionAPI()
    api_worker.start_consuming()