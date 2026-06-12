# 2주차 미니 파이프라인 비교 구현 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow와 Dagster로 P1 주문 수집 파이프라인(T1→T2→T3, SparkSession stub)을 각각 구현하고, 두 도구의 셋업·코드 방식·UI를 비교해 최종 도구를 선택한다.

**Architecture:** 독립 venv 전략(airflow/, dagster/ 각자 venv)으로 의존성 충돌 방지. SparkSession stub 함수는 shared/stubs.py에 단일 구현하고 양쪽에서 `pathlib.Path(__file__).parents[2]`로 참조. 비교 결과는 docs/study/week2/05-비교분석.md에 작성.

**Tech Stack:** apache-airflow==2.10.4, dagster, dagster-webserver, pyspark, pytest

---

## 파일 구조

```
orchestration/
├── shared/
│   ├── stubs.py                             # CREATE
│   └── tests/
│       ├── __init__.py                      # CREATE (빈 파일)
│       └── test_stubs.py                    # CREATE
├── airflow/
│   ├── requirements.txt                     # CREATE
│   └── dags/
│       └── p1_order_collection.py           # CREATE
├── dagster/
│   ├── requirements.txt                     # CREATE
│   ├── definitions.py                       # CREATE
│   └── p1_order_collection/
│       ├── __init__.py                      # CREATE (빈 파일)
│       └── assets.py                        # CREATE
└── docs/
    └── study/
        └── week2/
            └── 05-비교분석.md               # CREATE
```

---

## Task 1: 전제 조건 확인 + 저장소 골격 + shared/stubs.py

**Files:**
- Create: `shared/stubs.py`
- Create: `shared/tests/__init__.py`
- Create: `shared/tests/test_stubs.py`
- Create: `dagster/p1_order_collection/__init__.py`

- [ ] **Step 1: Java 11 설치 확인 및 JAVA_HOME 설정**

Working directory: 아무 곳이나 무방.

```bash
java -version
```

Expected: `openjdk version "11"` 이상 출력. 없으면:

```bash
brew install openjdk@11
sudo ln -sfn $(brew --prefix openjdk@11)/libexec/openjdk.jdk \
             /Library/Java/JavaVirtualMachines/openjdk-11.jdk
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
```

- [ ] **Step 2: 디렉토리 골격 생성**

Working directory: `orchestration/`

```bash
mkdir -p shared/tests airflow/dags dagster/p1_order_collection docs/study/week2
touch shared/tests/__init__.py dagster/p1_order_collection/__init__.py
```

- [ ] **Step 3: 테스트 먼저 작성 (TDD)**

Create `shared/tests/test_stubs.py`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1]))

import pytest
from stubs import spark_task_stub


def test_stub_completes():
    spark_task_stub("test_task", duration_sec=1, fail=False)


def test_stub_raises_on_fail():
    with pytest.raises(Exception, match="test_task intentionally failed"):
        spark_task_stub("test_task", duration_sec=0, fail=True)
```

- [ ] **Step 4: shared/stubs.py 구현**

Create `shared/stubs.py`:

```python
import os, sys, time
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def spark_task_stub(task_name: str, duration_sec: int = 10, fail: bool = False):
    spark = SparkSession.builder.appName(task_name).master("local[*]").getOrCreate()
    time.sleep(duration_sec)
    if fail:
        raise Exception(f"{task_name} intentionally failed")
    spark.stop()
```

- [ ] **Step 5: Commit (테스트는 Task 2에서 venv 설치 후 실행)**

```bash
git add shared/ dagster/p1_order_collection/__init__.py
git commit -m "feat: 저장소 골격 + shared/stubs.py 구현"
```

---

## Task 2: Airflow 환경 셋업 + stub 테스트 실행

**Files:**
- Create: `airflow/requirements.txt`

- [ ] **Step 1: requirements.txt 작성**

Create `airflow/requirements.txt`:

```
apache-airflow==2.10.4
pyspark
pytest
```

- [ ] **Step 2: venv 생성 및 패키지 설치**

Working directory: `orchestration/airflow/`

```bash
python -m venv .venv && source .venv/bin/activate

pip install apache-airflow==2.10.4 pyspark pytest \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"
```

Expected: `Successfully installed ...` (수 분 소요).

- [ ] **Step 3: AIRFLOW_HOME 영속화**

```bash
echo 'export AIRFLOW_HOME="$(dirname "$VIRTUAL_ENV")/.airflow"' >> .venv/bin/activate
source .venv/bin/activate
echo $AIRFLOW_HOME
```

Expected: `.../airflow/.airflow` 경로 출력.

- [ ] **Step 4: stub 테스트 실행**

Working directory: `orchestration/` (airflow venv 활성화 상태 유지)

```bash
cd ..
python -m pytest shared/tests/test_stubs.py -v
```

Expected:

```
PASSED shared/tests/test_stubs.py::test_stub_completes
PASSED shared/tests/test_stubs.py::test_stub_raises_on_fail
```

실패 시 체크: `echo $JAVA_HOME` 출력 확인, `java -version` 재확인.

- [ ] **Step 5: Airflow standalone 실행 및 UI 확인**

Working directory: `orchestration/airflow/`

```bash
source .venv/bin/activate
airflow standalone
```

브라우저에서 http://localhost:8080 접속. 콘솔에 출력된 임시 패스워드로 admin 로그인.

Expected: Airflow UI 대시보드 접속 성공.

- [ ] **Step 6: Commit**

```bash
git add airflow/requirements.txt
git commit -m "feat: Airflow 환경 셋업 완료 (SequentialExecutor + SQLite)"
```

---

## Task 3: Airflow P1 DAG 구현 + 실행 검증

**Files:**
- Create: `airflow/dags/p1_order_collection.py`

- [ ] **Step 1: DAG 파일 작성**

Create `airflow/dags/p1_order_collection.py`:

```python
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parents[2] / 'shared'))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from stubs import spark_task_stub

with DAG(
    dag_id="p1_order_collection",
    start_date=datetime(2026, 6, 1),
    schedule="0 * * * *",
    catchup=False,
) as dag:
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
    t1 >> t2 >> t3
```

- [ ] **Step 2: DAG 파싱 오류 없음 확인**

Working directory: `orchestration/airflow/`

```bash
source .venv/bin/activate
python dags/p1_order_collection.py
```

Expected: 아무 출력 없이 정상 종료.

- [ ] **Step 3: UI에서 DAG 수동 트리거**

`airflow standalone`이 실행 중인 상태에서 DAG Directory를 자동 감지. http://localhost:8080 → `p1_order_collection` DAG → Trigger DAG (▶ 버튼).

Expected:
- `collect_orders` → `normalize_data` → `load_to_dw` 순서로 실행
- 각 태스크 상태: success (초록)
- 전체 실행 시간: 약 23초 (10+5+8)

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/p1_order_collection.py
git commit -m "feat: Airflow P1 주문 수집 DAG 구현"
```

---

## Task 4: Airflow T3 선택적 재실행 검증 (UI)

코드 변경 없음. UI에서 T3만 재실행되는지 확인한다.

- [ ] **Step 1: T3 실패 시나리오 생성**

`airflow/dags/p1_order_collection.py`의 t3 `op_kwargs`에 `"fail": True` 추가:

```python
    t3 = PythonOperator(
        task_id="load_to_dw",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "load_to_dw", "duration_sec": 8, "fail": True},
    )
```

DAG 트리거 → `load_to_dw` 실패(빨간색) 확인.

- [ ] **Step 2: T3만 Clear & Re-run**

http://localhost:8080 → `p1_order_collection` → 실패한 Run → Graph 뷰 → `load_to_dw` 태스크 클릭 → **Clear** → confirm.

Expected:
- `collect_orders`, `normalize_data`는 재실행되지 않음 (success 상태 유지)
- `load_to_dw`만 재실행 → fail (fail=True이므로)

핵심 검증 포인트: T1, T2가 재실행되지 않았는지 확인.

- [ ] **Step 3: fail=True 제거 후 원복**

`op_kwargs`에서 `"fail": True` 제거:

```python
    t3 = PythonOperator(
        task_id="load_to_dw",
        python_callable=spark_task_stub,
        op_kwargs={"task_name": "load_to_dw", "duration_sec": 8},
    )
```

T3 Clear & Re-run → success 확인.

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/p1_order_collection.py
git commit -m "feat: Airflow T3 선택적 재실행 검증 완료 (fail 플래그 원복)"
```

---

## Task 5: Dagster 환경 셋업

**Files:**
- Create: `dagster/requirements.txt`
- Create: `dagster/definitions.py` (최소 골격)

- [ ] **Step 1: requirements.txt 작성**

Create `dagster/requirements.txt`:

```
dagster
dagster-webserver
pyspark
```

- [ ] **Step 2: venv 생성 및 패키지 설치**

새 터미널 탭에서 (Airflow standalone은 기존 탭에서 실행 중):

Working directory: `orchestration/dagster/`

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Expected: `Successfully installed ...`.

- [ ] **Step 3: 최소 definitions.py 작성**

Create `dagster/definitions.py`:

```python
from dagster import Definitions

defs = Definitions(assets=[])
```

- [ ] **Step 4: Dagster dev 실행 및 UI 확인**

```bash
dagster dev
# 자동 탐색 안 될 경우:
# dagster dev -f definitions.py
```

Expected: http://localhost:3000 Dagster UI 접속 성공.

- [ ] **Step 5: Commit**

```bash
git add dagster/requirements.txt dagster/definitions.py
git commit -m "feat: Dagster 환경 셋업 완료"
```

---

## Task 6: Dagster P1 Assets 구현 + materialization 검증

**Files:**
- Create: `dagster/p1_order_collection/assets.py`
- Modify: `dagster/definitions.py`

- [ ] **Step 1: assets.py 작성**

Create `dagster/p1_order_collection/assets.py`:

```python
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parents[2] / 'shared'))

from dagster import asset
from stubs import spark_task_stub


@asset
def orders_raw():
    spark_task_stub("orders_raw", duration_sec=10)


@asset(deps=[orders_raw])
def orders_normalized():
    spark_task_stub("orders_normalized", duration_sec=5)


@asset(deps=[orders_normalized])
def orders_dw():
    spark_task_stub("orders_dw", duration_sec=8)
```

- [ ] **Step 2: definitions.py 업데이트**

Modify `dagster/definitions.py`:

```python
from dagster import Definitions
from p1_order_collection.assets import orders_raw, orders_normalized, orders_dw

defs = Definitions(
    assets=[orders_raw, orders_normalized, orders_dw],
)
```

- [ ] **Step 3: 임포트 오류 없음 확인**

Working directory: `orchestration/dagster/`

```bash
source .venv/bin/activate
python -c "from p1_order_collection.assets import orders_raw, orders_normalized, orders_dw; print('OK')"
```

Expected: `OK`

- [ ] **Step 4: dagster dev 재시작 후 Asset catalog 확인**

`dagster dev` 프로세스 Ctrl+C 후 재실행. http://localhost:3000 → Assets 탭 → `orders_raw`, `orders_normalized`, `orders_dw` 3개 확인.

- [ ] **Step 5: 전체 Materialization 실행**

http://localhost:3000 → Assets 탭 → 3개 Asset 모두 선택 → **Materialize selected**

Expected:
- `orders_raw` → `orders_normalized` → `orders_dw` 순서로 실행
- 각 Asset 상태: Materialized (초록)
- 전체 실행 시간: 약 23초

- [ ] **Step 6: Commit**

```bash
git add dagster/p1_order_collection/assets.py dagster/definitions.py
git commit -m "feat: Dagster P1 주문 수집 Asset 구현"
```

---

## Task 7: Dagster orders_dw 선택적 재 materialization 검증 (UI)

코드 변경 없음.

- [ ] **Step 1: orders_dw만 단독 Materialize**

http://localhost:3000 → Assets 탭 → `orders_dw` 클릭 → **Materialize** (단일 asset 선택)

Expected:
- `orders_raw`, `orders_normalized`는 실행되지 않음
- `orders_dw`만 running → Materialized

- [ ] **Step 2: 실패 후 재실행 검증**

`dagster/p1_order_collection/assets.py`의 `orders_dw`에 `fail=True` 추가:

```python
@asset(deps=[orders_normalized])
def orders_dw():
    spark_task_stub("orders_dw", duration_sec=8, fail=True)
```

`dagster dev` 재시작 → `orders_dw` 단독 Materialize → 실패 확인 → `orders_dw`만 Re-materialize.

Expected: `orders_raw`, `orders_normalized` 재실행 없이 `orders_dw`만 재실행.

- [ ] **Step 3: fail=True 제거 후 원복**

```python
@asset(deps=[orders_normalized])
def orders_dw():
    spark_task_stub("orders_dw", duration_sec=8)
```

Materialize → success 확인.

- [ ] **Step 4: Commit**

```bash
git add dagster/p1_order_collection/assets.py
git commit -m "feat: Dagster orders_dw 선택적 재 materialization 검증 완료 (원복)"
```

---

## Task 8: 비교 분석 문서 작성

**Files:**
- Create: `docs/study/week2/05-비교분석.md`

- [ ] **Step 1: 템플릿 생성**

Create `docs/study/week2/05-비교분석.md`:

```markdown
# 2주차 비교 분석 — Airflow vs Dagster 미니 파이프라인 구현

> 구현 완료일: 2026-06-

---

## 1. 직접 구현으로 검증한 항목

### 셋업 난이도
| 항목 | Airflow | Dagster |
|---|---|---|
| 소요 시간 | | |
| 막힌 지점 | | |
| 핵심 명령어 | `airflow standalone` | `dagster dev` |

### 코드 작성 방식
| 항목 | Airflow | Dagster |
|---|---|---|
| 파이프라인 정의 | `with DAG(...) as dag:` | `@asset` 데코레이터 |
| 의존성 선언 | `t1 >> t2 >> t3` | `deps=[orders_raw]` |
| 실행 단위 | Task (행위) | Asset (결과물) |
| 체감 난이도 | | |

### Spark 연동
| 항목 | Airflow | Dagster |
|---|---|---|
| 연동 방식 | `PythonOperator` + SparkSession | `@asset` + SparkSession |
| 설정 복잡도 | | |

### 선택적 재실행 UI
| 항목 | Airflow | Dagster |
|---|---|---|
| 방법 | Task Clear & Re-run | Asset Re-materialize |
| 조작 편의성 | | |

---

## 2. 개념 기반 예측 항목 (1주차 학습 + 공식 문서)

### Pool(Airflow) vs Concurrency limit(Dagster)
- Airflow: Pool 슬롯 수 + `priority_weight` → 대기열 우선순위 정렬
- Dagster: Concurrency Key + `priority` → 동일 방식
- 예측: (Week 3~4에서 검증 예정)

### 우선순위 제어
- Airflow: Task 단위 `priority_weight`
- Dagster: Run/Job 단위 `priority`
- 예측: (Week 3~4에서 검증 예정)

### 크로스 파이프라인 의존성
- Airflow: `ExternalTaskSensor` — DAG ID/Task ID 직접 참조 (강결합)
- Dagster: `AssetSensor` — Asset key만 참조 (약결합)
- 예측: (Week 6~7에서 검증 예정)

---

## 3. 도구 선택

**선택:** (Airflow / Dagster)

**근거:**
1.
2.
3.

---

## 4. Week 3 이후 재검토 조건

-
```

- [ ] **Step 2: 실제 체험 내용으로 채우기**

위 템플릿의 빈 셀을 Task 1~7 수행 중 기록한 실제 경험으로 작성. 셋업 소요 시간, 막힌 지점, UI 조작 느낌, 코드 스타일 선호도 등.

- [ ] **Step 3: Commit**

```bash
git add docs/study/week2/05-비교분석.md
git commit -m "docs: 2주차 비교 분석 및 도구 선택 완료"
```

---

## 자기 검토

### 설계 문서 커버리지

| 설계 요구사항 | 대응 Task |
|---|---|
| 전제 조건 (Java, JAVA_HOME) | Task 1 Step 1 |
| shared/stubs.py | Task 1 Step 4 |
| stub 단위 테스트 | Task 1 Step 3, Task 2 Step 4 |
| Airflow 환경 셋업 | Task 2 |
| Airflow P1 DAG 구현 | Task 3 |
| Airflow T3 선택적 재실행 | Task 4 |
| Dagster 환경 셋업 | Task 5 |
| Dagster P1 Assets 구현 | Task 6 |
| Dagster orders_dw 선택적 재 materialization | Task 7 |
| 비교 분석 문서 + 도구 선택 | Task 8 |

### 완료 기준 대응

| 완료 기준 | Task |
|---|---|
| `airflow standalone` UI(8080) 접속 확인 (SequentialExecutor) | Task 2 Step 5 |
| `dagster dev` UI(3000) 접속 확인 | Task 5 Step 4 |
| Airflow P1 DAG 실행 성공 | Task 3 Step 3 |
| Dagster P1 Asset materialization 성공 | Task 6 Step 5 |
| T3 / orders_dw 단독 재실행 — 양쪽 UI 각 1회 | Task 4, Task 7 |
| `05-비교분석.md` 작성 완료 (도구 선택 포함) | Task 8 |
