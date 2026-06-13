from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "shared"))

from dagster import op, job

from stubs import spark_task_stub

# [우선순위/동시성 실험] Airflow priority_demo DAG과 동일한 구조의 Dagster 버전.
#
# 구성:
#   - op 3개가 서로 의존성 없음 → 동시에 실행 가능 상태
#   - 전부 같은 pool("spark")을 요청 → 풀 슬롯 제한(dagster.yaml에서 1로 설정)
#   - dagster/priority 태그를 다르게 → 슬롯이 빌 때 우선순위 순으로 소비되는지 관찰
#
# pool 슬롯 제한은 코드가 아니라 인스턴스 설정(dagster.yaml)에서 정한다.
#   → Airflow는 `airflow pools set spark_pool 1` CLI 한 줄,
#     Dagster는 DAGSTER_HOME + dagster.yaml 설정이 필요하다는 차이가 비교 포인트.


@op(pool="spark", tags={"dagster/priority": "10"})
def high_priority():
    spark_task_stub("high_priority", duration_sec=5)


@op(pool="spark", tags={"dagster/priority": "5"})
def mid_priority():
    spark_task_stub("mid_priority", duration_sec=5)


@op(pool="spark", tags={"dagster/priority": "1"})
def low_priority():
    spark_task_stub("low_priority", duration_sec=5)


@job
def priority_demo_job():
    # 세 op을 나란히 호출 — 서로 의존성이 없어 동시에 실행 가능 상태가 된다.
    high_priority()
    mid_priority()
    low_priority()
