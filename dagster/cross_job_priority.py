from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "shared"))

from dagster import op, job, In, Nothing

from stubs import spark_task_stub

# [크로스 잡 우선순위 실험] Airflow cross_dag_priority의 Dagster 버전.
#
# 검증 질문:
#   낮은 우선순위 job_a가 실행 중일 때 높은 우선순위 job_b를 실행하면,
#   전역 pool 슬롯(spark, 제한 1)을 두고 경쟁할 때 우선순위가 반영되는가?
#
# 주의(불확실성):
#   dagster/priority 태그는 "한 run 내부의 op 실행 순서"를 정한다.
#   서로 다른 run이 같은 pool 슬롯을 경쟁할 때(크로스 run) 우선순위가
#   반영되는지는 보장되지 않는다. 결과 자체가 비교 분석의 findings.
#
# 구성:
#   - 모든 op가 pool="spark" 공유 (dagster.yaml pools.default_limit=1)
#   - cross_job_a: xa→xb→xc, dagster/priority "1" (낮음)
#   - cross_job_b: xd→xe→xf, dagster/priority "100" (높음)
#   - op 간 순서는 In(Nothing) 의존성으로 연결 (데이터 전달 없는 순서 제약)

LOW = {"dagster/priority": "1"}
HIGH = {"dagster/priority": "100"}
AFTER = {"after": In(Nothing)}  # 데이터 없이 "선행 op 완료 후 실행" 의존성


# --- cross_job_a (낮은 우선순위): xa → xb → xc ---
@op(pool="spark", tags=LOW)
def xa():
    spark_task_stub("xa", duration_sec=10)


@op(pool="spark", tags=LOW, ins=AFTER)
def xb():
    spark_task_stub("xb", duration_sec=10)


@op(pool="spark", tags=LOW, ins=AFTER)
def xc():
    spark_task_stub("xc", duration_sec=10)


@job
def cross_job_a():
    xc(after=xb(after=xa()))


# --- cross_job_b (높은 우선순위): xd → xe → xf ---
@op(pool="spark", tags=HIGH)
def xd():
    spark_task_stub("xd", duration_sec=10)


@op(pool="spark", tags=HIGH, ins=AFTER)
def xe():
    spark_task_stub("xe", duration_sec=10)


@op(pool="spark", tags=HIGH, ins=AFTER)
def xf():
    spark_task_stub("xf", duration_sec=10)


@job
def cross_job_b():
    xf(after=xe(after=xd()))
