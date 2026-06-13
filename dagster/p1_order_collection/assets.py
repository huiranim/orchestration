from pathlib import Path
import sys

# 이 파일(dagster/p1_order_collection/)에서 두 단계 위가 저장소 루트(orchestration/).
# 그 아래 shared/ 를 파이썬 경로에 추가해 stubs 모듈을 import 한다.
# (Airflow DAG와 동일한 stub을 공유한다 — 두 도구가 같은 처리 로직을 부른다는 점이 비교의 전제)
sys.path.insert(0, str(Path(__file__).parents[2] / "shared"))

from dagster import asset

from stubs import spark_task_stub


# @asset: "이 함수는 하나의 데이터 자산을 만든다"는 선언.
# 함수 이름(orders_raw)이 곧 자산의 이름이 된다.
@asset
def orders_raw():
    """주문 원천 수집 — Airflow의 collect_orders에 대응."""
    spark_task_stub("orders_raw", duration_sec=10)


# deps=[orders_raw]: "이 자산을 만들려면 orders_raw가 먼저 있어야 한다"는 의존성 선언.
# Airflow의 t1 >> t2 와 같은 순서 제약이지만, 표현 방식이 "행위 순서"가 아니라
# "데이터 계보(lineage)"라는 점이 핵심 차이.
@asset(deps=[orders_raw])
def orders_normalized():
    """데이터 정규화 — Airflow의 normalize_data에 대응."""
    spark_task_stub("orders_normalized", duration_sec=5)


@asset(deps=[orders_normalized])
def orders_dw():
    """DW 적재 — Airflow의 load_to_dw에 대응. 시나리오 C의 선택적 재실행 대상."""
    spark_task_stub("orders_dw", duration_sec=8)
