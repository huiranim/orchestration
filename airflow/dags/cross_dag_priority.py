from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[2] / "shared"))

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from stubs import spark_task_stub

# [크로스 DAG 우선순위 실험] 시나리오 A의 핵심 질문 검증:
#   "낮은 우선순위 A가 실행 중일 때, 높은 우선순위 B를 트리거하면 어떻게 되나?"
#
# 구성:
#   - 두 DAG가 같은 pool("spark_pool", 슬롯 1개)을 공유
#   - cross_dag_a: a→b→c, 낮은 우선순위(absolute 1)
#   - cross_dag_b: d→e→f, 높은 우선순위(absolute 100)
#
# 관찰 시나리오:
#   1. cross_dag_a 트리거 → a, b 가 차례로 슬롯 점유하며 실행
#   2. b 실행 중에 cross_dag_b 트리거 → d 는 슬롯이 없어 대기 (b를 멈추지 못함 = 선점 X)
#   3. b 완료로 슬롯이 비면, 대기 중이던 c(A)와 d(B)가 경쟁
#      → 우선순위 높은 d(B)가 먼저 실행되는지 확인 (우선순위 정렬 O)
#
# weight_rule="absolute": priority_weight를 하위 태스크 합산 없이 적힌 값 그대로 사용.

COMMON = dict(start_date=datetime(2026, 6, 1), schedule=None, catchup=False)


def _op(task_id: str, weight: int) -> PythonOperator:
    """pool/우선순위 설정이 동일한 PythonOperator 생성 헬퍼."""
    return PythonOperator(
        task_id=task_id,
        python_callable=spark_task_stub,
        op_kwargs={"task_name": task_id, "duration_sec": 10},
        pool="spark_pool",
        priority_weight=weight,
        weight_rule="absolute",
    )


# 낮은 우선순위 DAG: a → b → c
with DAG(dag_id="cross_dag_a", **COMMON):
    _op("a", 1) >> _op("b", 1) >> _op("c", 1)

# 높은 우선순위 DAG: d → e → f
with DAG(dag_id="cross_dag_b", **COMMON):
    _op("d", 100) >> _op("e", 100) >> _op("f", 100)
