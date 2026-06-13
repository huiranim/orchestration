from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[2] / "shared"))

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from stubs import spark_task_stub

# [우선순위/동시성 실험] 시나리오 A/B의 축소판.
#
# 구성:
#   - 태스크 3개가 서로 의존성 없음 → 동시에 실행 가능 상태가 됨
#   - 전부 같은 pool("spark_pool", 슬롯 1개)을 요청 → 한 번에 1개만 실행 가능
#   - priority_weight를 다르게 설정 → 슬롯이 빌 때 어떤 순서로 소비되는지 관찰
#
# 기대: 슬롯이 1개뿐이라 high → mid → low 순서로 실행된다.
#       (didimdp_manager의 FIFO와 달리, 우선순위로 대기열이 정렬되는지 검증)
with DAG(
    dag_id="priority_demo",
    start_date=datetime(2026, 6, 1),
    schedule=None,          # 수동 트리거 전용 (스케줄 없음)
    catchup=False,
) as dag:
    high = PythonOperator(
        task_id="high_priority",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "high_priority", "duration_sec": 5},
        pool="spark_pool",
        priority_weight=10,
    )
    mid = PythonOperator(
        task_id="mid_priority",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "mid_priority", "duration_sec": 5},
        pool="spark_pool",
        priority_weight=5,
    )
    low = PythonOperator(
        task_id="low_priority",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "low_priority", "duration_sec": 5},
        pool="spark_pool",
        priority_weight=1,
    )

    # 의존성 선언 없음 — 세 태스크는 동시에 "실행 가능" 상태가 되고,
    # 슬롯 경쟁에서 priority_weight 순으로 소비되는지가 관찰 대상.
