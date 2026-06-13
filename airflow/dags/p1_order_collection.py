from pathlib import Path
import sys

# 이 파일(airflow/dags/)에서 두 단계 위가 저장소 루트(orchestration/).
# 그 아래 shared/ 를 파이썬 경로에 추가해 stubs 모듈을 import 한다.
sys.path.insert(0, str(Path(__file__).parents[2] / "shared"))

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from stubs import spark_task_stub

# with 블록 안에서 정의된 모든 태스크는 자동으로 이 DAG에 소속된다.
with DAG(
    dag_id="p1_order_collection",
    start_date=datetime(2026, 6, 1),   # 과거 날짜 — 스케줄 시작 기준점
    schedule="0 * * * *",              # cron: 매시간 정각
    catchup=False,                     # 과거 구간 소급 실행 안 함 (필수)
) as dag:
    # PythonOperator: 파이썬 함수를 실행하는 태스크
    # python_callable: 실행할 함수
    # op_kwargs: 그 함수에 넘길 인자 (딕셔너리)
    t1 = PythonOperator(
        task_id="collect_orders",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "collect_orders", "duration_sec": 10},
    )
    t2 = PythonOperator(
        task_id="normalize_data",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "normalize_data", "duration_sec": 5},
    )
    t3 = PythonOperator(
        task_id="load_to_dw",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "load_to_dw", "duration_sec": 8},
    )

    # >> 연산자로 실행 순서(의존성)를 선언: t1 다음 t2, 그 다음 t3
    t1 >> t2 >> t3
