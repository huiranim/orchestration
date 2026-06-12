import os
import sys
import time

from pyspark.sql import SparkSession

# Airflow가 PythonOperator 태스크를 서브프로세스로 실행할 때,
# PySpark가 Python 워커를 스폰하면서 시스템 Python을 참조할 수 있음.
# 현재 venv의 Python 경로를 명시적으로 지정해 일치시킨다.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def spark_task_stub(task_name: str, duration_sec: int = 10, fail: bool = False):
    """
    오케스트레이터 학습용 Spark 태스크 스텁.
    실제 비즈니스 로직 없이 실행 시간만 시뮬레이션한다.

    Args:
        task_name:    SparkSession 앱 이름 (로그에 표시됨)
        duration_sec: 처리 시간 시뮬레이션 (초)
        fail:         True이면 의도적으로 예외 발생
    """
    # SparkSession.builder: Spark 실행 환경 설정
    # appName: Spark UI / 로그에서 이 태스크를 식별하는 이름
    # master("local[*]"): 클러스터 없이 로컬 CPU 전체 코어 사용
    # getOrCreate(): 이미 실행 중인 세션이 있으면 재사용
    spark = SparkSession.builder.appName(task_name).master("local[*]").getOrCreate()

    # pytest 실행 시 Spark INFO 로그가 수십 줄 출력되어 PASS/FAIL을 가림.
    # WARN 이상만 출력하도록 낮춤.
    spark.sparkContext.setLogLevel("WARN")

    try:
        time.sleep(duration_sec)  # 처리 시간 시뮬레이션
        if fail:
            raise Exception(f"{task_name} intentionally failed")
    finally:
        # 예외 발생 여부와 무관하게 항상 세션 종료
        # 없으면 fail=True 경로에서 세션이 닫히지 않아 다음 테스트에 영향
        spark.stop()
