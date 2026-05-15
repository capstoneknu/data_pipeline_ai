import pandas as pd
import time
import logging
import os
from pathlib import Path
from influxdb_client import InfluxDBClient, WriteOptions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [BULK-INGEST] - %(levelname)s - %(message)s')

class InfluxBulkIngestor:
    """
    [AI 초기 학습 및 부팅용 대용량 데이터 적재기]
    가상 센서 스트리밍 전, 과거 30일치 데이터(1만 가구)를 InfluxDB에 한 번에 들이부어
    AI 모델이 과거 96칸의 데이터를 조회(get_recent_history)할 수 있도록 채우는 역할.
    """
    def __init__(self):
        self.url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        self.token = os.getenv('INFLUXDB_TOKEN', 'super-secret-capstone-token')
        self.org = os.getenv('INFLUXDB_ORG', 'khnp-dr')
        self.bucket = os.getenv('INFLUXDB_BUCKET', 'power-data')
        
        current_dir = Path(__file__).resolve().parent
        self.csv_path = current_dir.parent / "data" / "final_synthetic_ami_data.csv"

        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        
        # Out of Memory 방어를 위한 백그라운드 5만 건 강제 플러시 엔진 유지
        self.write_api = self.client.write_api(write_options=WriteOptions(
            batch_size=50000, 
            flush_interval=1000,
            jitter_interval=0,
            retry_interval=5000,
            max_retries=3,
            max_retry_delay=15000,
            exponential_base=2
        ))

    def run_bulk_insert(self):
        if not self.csv_path.exists():
            logging.error(f"데이터 파일 누락: {self.csv_path} (먼저 generate_dr_data.py 실행 필요)")
            return

        logging.info("1만 가구 대규모 시계열 데이터(Bulk Ingestion) 적재를 시작합니다...")
        start_time = time.time()
        
        chunk_size = 200000  # 20만 줄 단위 청크 I/O
        total_inserted = 0

        try:
            for chunk_idx, df_chunk in enumerate(pd.read_csv(self.csv_path, chunksize=chunk_size)):
                df_chunk.rename(columns={'user_id': 'device_id'}, inplace=True)
                df_chunk['timestamp'] = pd.to_datetime(df_chunk['timestamp'], utc=True)
                df_chunk.set_index('timestamp', inplace=True)

                self.write_api.write(
                    bucket=self.bucket,
                    org=self.org,
                    record=df_chunk,
                    data_frame_measurement_name='power_usage',
                    data_frame_tag_columns=['device_id'] 
                )
                
                total_inserted += len(df_chunk)
                logging.info(f"⚡ {chunk_idx + 1}번째 청크 적재 완료... (누적: {total_inserted:,} 건)")

        except Exception as e:
            logging.error(f"적재 중 오류 발생: {e}")
        finally:
            self.write_api.flush()
            self.write_api.close()
            self.client.close()
            
            elapsed_time = time.time() - start_time
            logging.info(f"데이터 구축 완료! (총 {total_inserted:,} 건 / 소요 시간: {elapsed_time:.2f} 초)")

if __name__ == "__main__":
    ingestor = InfluxBulkIngestor()
    ingestor.run_bulk_insert()