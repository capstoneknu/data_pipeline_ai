import asyncio
import json
import time
import logging
import math
import random
from datetime import datetime, timezone
import statistics

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
        
        logging.info("="*50)
        logging.info("대규모 트래픽 부하 테스트 결과 (SLA 200ms 검증)")
        logging.info("="*50)
        logging.info(f"총 소요 시간     : {total_time:.2f} 초")
        logging.info(f"전송 성공 (Acks) : {self.success_count:,} 건")
        logging.info(f"초당 처리량(TPS) : {tps:,.2f} Msg/sec")
        
        if self.latencies:
            avg_lat = statistics.mean(self.latencies)
            p99_lat = statistics.quantiles(self.latencies, n=100)[-1] if len(self.latencies) >= 100 else max(self.latencies)
            logging.info("-" * 50)
            logging.info(f"최소 지연 시간   : {min(self.latencies):.2f} ms")
            logging.info(f"평균 지연 시간   : {avg_lat:.2f} ms")
            logging.info(f"최대 지연 시간   : {max(self.latencies):.2f} ms")
            logging.info(f"P99 지연 시간 : {p99_lat:.2f} ms (하위 1% 악성 지연)")
            
            # SLA 엄격 검증
            if avg_lat <= 200:
                logging.info("[SLA 통과] 평균 지연 시간 200ms 이하 달성 완료!")
            else:
                logging.error("[SLA 실패] 평균 지연 시간이 200ms를 초과했습니다.")
        logging.info("="*50)

# 네트워크 I/O 응답(Ack) 대기 전용 경량 코루틴
async def measure_ack(future, start_time, metrics):
    try:
        await future
        latency_ms = (time.perf_counter() - start_time) * 1000
        metrics.record(latency_ms, success=True)
    except Exception:
        metrics.record(0, success=False)


async def run_stress_test(num_nodes: int = 10000):
    topic = 'power-telemetry-topic'
    
    # CPU 병목 제거 및 IoT 표준 Acks 정책 적용
    producer = AIOKafkaProducer(
        bootstrap_servers='127.0.0.1:9092',
        acks=1,                  # Leader Ack만 확인 (응답 속도 2배 향상)
        compression_type=None,   # 로컬 파이썬 GIL 압축 병목 원천 제거
        linger_ms=20,            # 패킷 최적화를 위한 20ms 마이크로 대기
        max_batch_size=1048576   # 배치 크기 1MB로 확장 (TCP 오버헤드 최소화)
    )
    
    await producer.start()
    logging.info("Kafka Broker 연결 성공.")
    
    logging.info("[Warm-up] 토픽 메타데이터 초기화를 위해 더미 데이터를 발송합니다...")
    await producer.send_and_wait(topic, key=b"warmup", value=b"{}")
    logging.info("[Warm-up] 메타데이터 캐싱 완료.")
    
    logging.info(f"{num_nodes:,}개의 가상 노드 Payload를 메모리에 사전 생성 중...")
    payloads = []
    hour = datetime.now(timezone.utc).hour
    for i in range(num_nodes):
        node_id = f"VIRTUAL_USER_{str(i).zfill(5)}"
        base_load = 0.05 
        activity_curve = max(0, math.sin(math.pi * (hour - 6) / 18)) * 0.4 
        noise = random.uniform(-0.02, 0.02)
        total_kwh = max(0.01, base_load + activity_curve + noise)

        payload_dict = {
            "user_id": node_id, "power_kwh": round(total_kwh, 3),
            "timestamp": time.time(), "source": "stress_test_simulator"
        }
        # JSON 직렬화 연산(CPU)도 미리 다 끝내버림
        payloads.append((node_id.encode('utf-8'), json.dumps(payload_dict).encode('utf-8')))
        
    logging.info("Payload 생성 완료. 순수 네트워크 I/O 부하 테스트를 시작합니다.")

    metrics = LoadTestMetrics()
    metrics.start_time = time.perf_counter()
    
    # 컨텍스트 스위칭 붕괴 차단
    # 메인 스레드에서 카프카 버퍼로 데이터를 빛의 속도로 밀어넣고,
    # 백그라운드 태스크는 조용히 Ack만 기다립니다.
    ack_tasks = []
    for key_bytes, value_bytes in payloads:
        req_start = time.perf_counter()
        
        # CPU 제어권 양보 없이 즉시 내부 메모리 버퍼로 적재 (Send)
        future = await producer.send(topic, key=key_bytes, value=value_bytes)
        
        # 백그라운드에서 브로커 응답을 기다리는 경량 태스크 할당
        ack_tasks.append(asyncio.create_task(measure_ack(future, req_start, metrics)))
    
    # 모든 브로커의 Ack가 돌아올 때까지 동시 대기
    await asyncio.gather(*ack_tasks)
    
    metrics.end_time = time.perf_counter()
    await producer.stop()
    metrics.print_report()

if __name__ == "__main__":
    asyncio.run(run_stress_test(num_nodes=10000))