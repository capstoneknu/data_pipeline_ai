import asyncio
import json
import time
import logging
import math
import random
from datetime import datetime, timezone
import statistics

# 비동기 초고속 카프카 프로듀서
from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [STRESS-TEST] - %(message)s')

class LoadTestMetrics:
    def __init__(self):
        self.latencies = []
        self.success_count = 0
        self.fail_count = 0
        self.start_time = 0
        self.end_time = 0

    def record(self, latency_ms, success=True):
        if success:
            self.latencies.append(latency_ms)
            self.success_count += 1
        else:
            self.fail_count += 1

    def print_report(self):
        total_time = self.end_time - self.start_time
        total_requests = self.success_count + self.fail_count
        tps = total_requests / total_time if total_time > 0 else 0
        
        logging.info("="*55)
        logging.info("대규모 트래픽 부하 테스트 결과 (SLA 200ms 검증)")
        logging.info("="*55)
        logging.info(f"총 소요 시간     : {total_time:.3f} 초")
        logging.info(f"전송 성공 (Acks) : {self.success_count:,} 건")
        logging.info(f"초당 처리량(TPS) : {tps:,.2f} Msg/sec")
        
        if self.latencies:
            avg_lat = statistics.mean(self.latencies)
            p99_lat = statistics.quantiles(self.latencies, n=100)[-1] if len(self.latencies) >= 100 else max(self.latencies)
            logging.info("-" * 55)
            logging.info(f"최소 지연 시간   : {min(self.latencies):.2f} ms")
            logging.info(f"평균 지연 시간   : {avg_lat:.2f} ms")
            logging.info(f"최대 지연 시간   : {max(self.latencies):.2f} ms")
            logging.info(f"P99 지연 시간    : {p99_lat:.2f} ms (하위 1% 악성 지연)")
            
            # SLA 엄격 검증
            if avg_lat <= 200:
                logging.info("\n[SLA 통과] 평균 지연 시간 200ms 이하 달성 완료!")
            else:
                logging.error("\n[SLA 실패] 평균 지연 시간이 200ms를 초과했습니다.")
        logging.info("="*55)

async def measure_ack(future, start_time, metrics):
    try:
        await future
        latency_ms = (time.perf_counter() - start_time) * 1000
        metrics.record(latency_ms, success=True)
    except Exception as e:
        logging.error(f"Ack 대기 중 에러 발생: {e}")
        metrics.record(0, success=False)

async def run_stress_test(num_nodes: int = 10000):
    # [수정] A파트 및 B파트가 공통으로 바라보는 핵심 Persistence 토픽으로 타겟 변경
    topic = 'power-usage-topic'
    
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        acks=1,                  
        compression_type=None,   
        linger_ms=20,            
        max_batch_size=1048576   
    )
    
    await producer.start()
    logging.info("Kafka Broker 연결 성공.")
    
    logging.info("[Warm-up] 토픽 메타데이터 초기화를 위해 더미 데이터를 발송합니다...")
    await producer.send_and_wait(topic, key=b"warmup", value=b"{}")
    logging.info("[Warm-up] 메타데이터 캐싱 완료.")
    
    logging.info(f"{num_nodes:,}개의 가상 노드 Payload를 메모리에 사전 직렬화 중 (CPU 오버헤드 제거)...")
    payloads = []
    
    # 시간 동기화 (ISO 8601 포맷)
    current_dt_iso = datetime.now(timezone.utc).isoformat()
    
    for i in range(num_nodes):
        # [수정] 식별자 정규화: "1"번은 MySQL 타격, "2~10000"번은 InfluxDB 타격 (Spring Boot 드롭 필터 활용)
        node_id_str = str(i + 1)
        kwh_val = round(max(0.01, 0.05 + random.uniform(-0.02, 0.02)), 3)

        # [수정] A파트/B파트 공통 Contract Schema 적용
        payload_dict = {
            "device_id": node_id_str,
            "timestamp": current_dt_iso,
            "kwh_usage": kwh_val
        }
        
        payloads.append((node_id_str.encode('utf-8'), json.dumps(payload_dict).encode('utf-8')))
        
    logging.info("Payload 생성 완료. 카프카를 향한 순수 네트워크 I/O 타격을 개시합니다.")

    metrics = LoadTestMetrics()
    metrics.start_time = time.perf_counter()
    
    ack_tasks = []
    for key_bytes, value_bytes in payloads:
        req_start = time.perf_counter()
        # Non-blocking Send (메모리 버퍼로 즉시 푸시)
        future = await producer.send(topic, key=key_bytes, value=value_bytes)
        ack_tasks.append(asyncio.create_task(measure_ack(future, req_start, metrics)))
    
    # 카프카 브로커의 전체 응답 동시 대기
    await asyncio.gather(*ack_tasks)
    
    metrics.end_time = time.perf_counter()
    await producer.stop()
    metrics.print_report()

if __name__ == "__main__":
    # 1만 가구 규모의 극단적 트래픽 설정
    asyncio.run(run_stress_test(num_nodes=10000))