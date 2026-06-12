# 2주차 설계 — 미니 파이프라인 비교 구현 및 도구 선택

> 브레인스토밍 결과 기록 (2026-06-12)
> 명세서 §5 2주차 일정 기반: 환경 셋업(2h) + Airflow 구현(3h) + Dagster 구현(3h) + 비교 분석(2h)

---

## 1. 핵심 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Airflow 실행 방식 | `airflow standalone` (SQLite + SequentialExecutor) | Docker 불필요, 2h 내 셋업 목표. standalone은 SequentialExecutor로 뜸 — P1 순차 파이프라인에는 충분. Redis/Worker는 Week 3~4 시 추가 |
| Dagster 실행 방식 | `dagster dev` | `dagster` 패키지 내장 공식 개발 서버 명령어 |
| DB | SQLite (airflow standalone 기본값) | Postgres/MySQL 컨테이너 불필요 |
| 환경 격리 | 각자 독립 venv | airflow와 dagster 의존성 충돌 방지 |
| 미니 파이프라인 범위 | P1 T1→T2→T3 + SparkSession 포함, Pool/우선순위 제외 | 명세서 "실제 SparkSession 생성" 원칙 준수. 우선순위·동시성은 Week 3~4 |
| shared stub | `shared/stubs.py` 단일 파일 공유 | 중복 제거. `sys.path` 직접 추가로 참조 |

---

## 2. 저장소 디렉토리 구조

```
orchestration/
├── airflow/
│   ├── dags/
│   │   └── p1_order_collection.py   # DAG 정의 (T1→T2→T3 의존성)
│   ├── .env                          # AIRFLOW_HOME 등 환경변수
│   └── requirements.txt             # apache-airflow, pyspark
├── dagster/
│   ├── p1_order_collection/
│   │   ├── __init__.py
│   │   └── assets.py                # @asset 정의
│   ├── definitions.py               # Definitions() 엔트리포인트
│   └── requirements.txt             # dagster, dagster-webserver, pyspark
├── shared/
│   └── stubs.py                     # spark_task_stub() 공통 함수
└── docs/
    ├── specs/                        # 과제 명세서
    ├── study/
    │   ├── week1/                    # 1주차 학습 노트
    │   └── week2/
    │       └── 05-비교분석.md        # 2주차 최종 비교 산출물
    └── record/
        └── week2/
            └── 2026-06-12-week2-design.md   # 이 파일
```

---

## 3. 환경 셋업 순서

### 전제 조건
- Java 11+ 설치 확인: `java -version` (없으면 `brew install openjdk@11`)
  - `brew install` 후 출력되는 caveats의 symlink 명령을 먼저 실행해야 `/usr/libexec/java_home`이 JDK를 인식함:
    ```bash
    sudo ln -sfn $(brew --prefix openjdk@11)/libexec/openjdk.jdk \
                 /Library/Java/JavaVirtualMachines/openjdk-11.jdk

    export JAVA_HOME=$(/usr/libexec/java_home -v 11)
    # 영속화: ~/.zshrc에 추가
    ```
- Python 3.11+ 설치 확인: `python --version`

### Airflow
```bash
cd airflow/
python -m venv .venv && source .venv/bin/activate

# 버전 고정 + 공식 constraints 파일로 의존성 충돌 방지
pip install apache-airflow==2.10.4 pyspark \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

# AIRFLOW_HOME 영속화 — venv activate 스크립트에 추가해두면 터미널 재시작 후에도 유지
echo 'export AIRFLOW_HOME="$(dirname "$VIRTUAL_ENV")/.airflow"' >> .venv/bin/activate
source .venv/bin/activate  # 재로드

airflow standalone
# → http://localhost:8080 (admin / 콘솔에 출력되는 임시 패스워드)
```

### Dagster
```bash
cd dagster/
python -m venv .venv && source .venv/bin/activate
pip install dagster dagster-webserver pyspark

dagster dev
# → http://localhost:3000
```

### shared/stubs.py 참조 방법
Airflow `dags/p1_order_collection.py`와 Dagster `p1_order_collection/assets.py` 모두 저장소 루트(`orchestration/`)까지 정확히 2단계 상위. 동일한 한 줄로 처리:
```python
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parents[2] / 'shared'))
from stubs import spark_task_stub
```
- `airflow/dags/p1.py` → parents[2] = `orchestration/` ✓
- `dagster/p1_order_collection/assets.py` → parents[2] = `orchestration/` ✓

---

## 4. 미니 파이프라인 구현 범위

### 대상: P1 주문 수집 파이프라인
```
T1. 주문 원천 수집  (stub: 10s)
T2. 데이터 정규화   (stub: 5s)
T3. DW 적재         (stub: 8s)
```
의존성: T1 → T2 → T3 (순차)

### shared/stubs.py 공통 패턴
```python
import os, sys, time
from pyspark.sql import SparkSession

# venv Python을 PySpark 드라이버로 고정 (Airflow 서브프로세스 환경에서 시스템 Python 참조 방지)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

def spark_task_stub(task_name: str, duration_sec: int = 10, fail: bool = False):
    spark = SparkSession.builder.appName(task_name).master("local[*]").getOrCreate()
    time.sleep(duration_sec)
    if fail:
        raise Exception(f"{task_name} intentionally failed")
    spark.stop()
```

### 이번 주 구현 범위 외 항목
- Pool / priority_weight / Concurrency Key → Week 3~4
- ExternalTaskSensor / AssetSensor → Week 6~7
- P2, P3, P4 → Week 3 이후

---

## 5. 비교 분석 문서 구조 (`docs/study/week2/05-비교분석.md`)

```markdown
## 1. 직접 구현으로 검증한 항목
- 셋업 난이도 (체감 시간, 막힌 지점 기록)
- 코드 작성 방식 (`>>` vs `deps=[]`, Operator vs @asset)
- Spark 연동 방식 (PythonOperator+SparkSession vs @asset+SparkSession)
- 선택적 재실행 UI (T3 Clear & Re-run vs T3 Re-materialize 직접 실행)

## 2. 개념 기반 예측 항목 (1주차 학습 + 공식 문서)
- Pool(Airflow) vs Concurrency limit(Dagster): 슬롯 수 설정 방식과 대기열 우선순위 정렬 동작 → Week 3~4에서 검증 예정 (시나리오 A/B 핵심)
- 우선순위 제어: `priority_weight`(Airflow) vs `priority`(Dagster) → Week 3~4에서 검증 예정
- 크로스 파이프라인 의존성: ExternalTaskSensor vs AssetSensor → Week 6~7에서 검증 예정

## 3. 도구 선택 및 근거
- 선택: [Airflow / Dagster]
- 핵심 근거 3줄 이내

## 4. Week 3 이후 재검토 조건
- 어떤 상황이면 선택을 뒤집을 수 있는가
```

---

## 6. 2주차 완료 기준

- [ ] `airflow standalone` UI(8080) 접속 확인 (Executor: SequentialExecutor)
- [ ] `dagster dev` UI(3000) 접속 확인 (자동 탐색 안 될 경우 `dagster dev -f definitions.py`)
- [ ] Airflow P1 DAG (T1→T2→T3 SparkSession stub) 실행 성공
  - 주의: DAG 정의 시 `catchup=False` 필수 — 누락 시 과거 Run이 대량 생성됨
- [ ] Dagster P1 Asset (동일 내용) materialization 성공
- [ ] T3만 단독 재실행 — 양쪽 UI에서 각각 1회씩 수행
- [ ] `docs/study/week2/05-비교분석.md` 작성 완료 (도구 선택 포함)
