# 3주차 구현 계획 — Dagster 4개 파이프라인 골격 + 시나리오 사전검증 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 설계 문서(`docs/record/week3/2026-06-21-week3-scenario-design.md`)대로 은행 도메인 4개 파이프라인(P1~P4)을 Dagster 자산으로 구현하고, A/B/C 시나리오를 사전 검증한다.

**Architecture:** 1 태스크 = 1 자산(데이터 중심 이름). `transactions_dw`가 P2·P3 공통 상류. P2는 Declarative Automation(eager), P3는 AssetSensor + 수동, P1·P4는 스케줄. 슬롯은 `pool="spark"`(limit 3, granularity op), P4 학습은 `@graph_asset` 내부 병렬 op 2개로 2슬롯 점유. 우선순위는 run 태그 `dagster/priority` + QueuedRunCoordinator(Phase 1).

**Tech Stack:** dagster 1.13.9, pyspark 4.1.2, pytest, Python 3.11 (`dagster/.venv`).

## Global Constraints

- 모든 명령·테스트는 **`dagster/.venv`** 활성화 상태에서 실행 (`source dagster/.venv/bin/activate`).
- 태스크 스텁은 **실제 `SparkSession` 생성** 유지 (`shared/stubs.py`의 `spark_task_stub`). 비즈니스 로직 없음.
- 슬롯 = `pool="spark"` (limit=3, **granularity=op**). **모든 spark 태스크 자산이 `pool="spark"`를 요청**한다(체인 순차 실행이라 파이프라인당 동시 1슬롯). P4 T3만 내부 병렬 op 2개로 2슬롯. ← *이게 빠지면 풀 수요가 항상 2뿐이라 시나리오 A/B 재현 불가.*
- 우선순위 값(run 태그 `dagster/priority`): **P1=2, P2=1, P3=3, P4=0**.
- 자산 이름은 **데이터 중심**(행위 아님). 파이프라인별 `group_name`: p1/p2/p3/p4.
- 태스크 duration(초): 설계 §2-3 표 값. fail 토글이 필요한 자산: **transactions_raw, transactions_dw, summary_validated** (run config `fail`).
- md/코드 파일명은 영어. 주석·문서 본문은 한국어 허용.
- 커밋만 수행, 푸시는 사용자 요청 시에만. 커밋 메시지 끝에 `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## 파일 구조 (목표)

```
dagster/
  definitions.py                       # 진입점 (수정: shared 경로 부트스트랩 + 전체 등록)
  pipelines/
    __init__.py                        # (신규, 빈 파일)
    stub_config.py                     # (신규) StubConfig(fail: bool)
    p1_transactions/__init__.py, assets.py   # transactions_raw → _normalized → _dw
    p2_aggregation/__init__.py, assets.py    # txn_loaded → daily_summary → summary_dw → summary_validated (eager)
    p3_settlement/__init__.py, assets.py     # settlement_base → ... → settlement_file
    p4_fds/__init__.py, assets.py            # behavior_logs → fds_features → fds_model(graph_asset) → ...
  jobs.py                              # (신규) 파이프라인별 asset job + priority 태그
  schedules.py                         # (신규) P1 매시간, P4 매주 월 02:00
  sensors.py                           # (신규) P3 AssetSensor + 자동화 센서
  experiments/__init__.py              # (신규, 빈) 2주차 실험 이동 대상
  tests/__init__.py, conftest.py, test_*.py  # (신규) 자산/정의 테스트
  .dagster_home/dagster.yaml           # (수정) pool/granularity/QueuedRunCoordinator
  requirements.txt                     # (수정) pytest 추가
```

**임포트 부트스트랩 규칙(중요):** pipeline assets는 `from stubs import spark_task_stub`(shared)와 `from pipelines.stub_config import StubConfig`(dagster 루트)를 한다. 따라서 `dagster/`(pipelines.* 용)와 `shared/`(stubs 용)가 sys.path에 있어야 한다. `dagster/`는 dagster 로더가 `-f definitions.py` 로드 시 자동 추가하고, `shared/`는 **definitions.py 상단**과 **tests/conftest.py**에서 명시 추가한다. → 개별 assets.py에는 sys.path 조작을 넣지 않는다(기존 p1 패턴 대체).

---

## Task 1: 테스트 환경 셋업 (pytest + conftest)

기존 코드는 건드리지 않고 dagster venv에서 pytest가 동작하도록 만든다.

**Files:**
- Modify: `dagster/requirements.txt`
- Create: `dagster/tests/__init__.py` (빈 파일)
- Create: `dagster/tests/conftest.py`

**Interfaces:**
- Produces: conftest의 autouse 픽스처 `_no_sleep`(테스트 중 `time.sleep` no-op), sys.path에 `dagster/`·`shared/` 추가.

- [ ] **Step 1: requirements.txt에 pytest 추가**

`dagster/requirements.txt`를 다음으로 만든다:
```
dagster
dagster-webserver
pyspark
pytest
```

- [ ] **Step 2: pytest 설치**

Run: `source dagster/.venv/bin/activate && pip install -r dagster/requirements.txt`
Expected: `Successfully installed pytest-...` (이미 있으면 already satisfied)

- [ ] **Step 3: conftest 작성**

`dagster/tests/__init__.py` = 빈 파일. `dagster/tests/conftest.py`:
```python
import sys
from pathlib import Path

import pytest

# dagster/ (pipelines.* 임포트용) 와 shared/ (stubs 임포트용) 를 경로에 추가
_ROOT = Path(__file__).parents[1]            # dagster/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT.parent / "shared"))


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """테스트 속도 확보: 스텁의 time.sleep 을 no-op 으로. SparkSession 생성은 유지."""
    import stubs
    monkeypatch.setattr(stubs.time, "sleep", lambda *a, **k: None)
```

- [ ] **Step 4: 기존 스텁 테스트가 dagster venv에서 통과하는지 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest shared/tests/test_stubs.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add dagster/requirements.txt dagster/tests/__init__.py dagster/tests/conftest.py
git commit -m "test: dagster venv에 pytest 추가 + tests/conftest 셋업

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: P1 은행 도메인 전환 + 디렉토리 재구성

`p1_order_collection/`을 `pipelines/p1_transactions/`로 옮기고 은행 도메인 이름으로 바꾼다. 2주차 실험을 `experiments/`로 이동. definitions.py 임포트 갱신.

**Files:**
- Create: `dagster/pipelines/__init__.py` (빈), `dagster/pipelines/stub_config.py`, `dagster/pipelines/p1_transactions/__init__.py` (빈), `dagster/pipelines/p1_transactions/assets.py`
- Create: `dagster/experiments/__init__.py` (빈)
- Move (git mv): `dagster/priority_demo_job.py` → `dagster/experiments/priority_demo_job.py`, `dagster/cross_job_priority.py` → `dagster/experiments/cross_job_priority.py`
- Delete: `dagster/p1_order_collection/` (이동 후 제거)
- Modify: `dagster/definitions.py`
- Create: `dagster/tests/test_p1_transactions.py`

**Interfaces:**
- Produces: 자산 `transactions_raw`, `transactions_normalized`, `transactions_dw` (group `p1`). `StubConfig(fail: bool = False)`. `transactions_raw`·`transactions_dw`는 `config: StubConfig`를 받는다.

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_p1_transactions.py`:
```python
from dagster import materialize

from pipelines.p1_transactions.assets import (
    transactions_raw,
    transactions_normalized,
    transactions_dw,
)

P1 = [transactions_raw, transactions_normalized, transactions_dw]


def test_p1_chain_materializes():
    result = materialize(P1)
    assert result.success


def test_p1_groups_are_p1():
    assert transactions_raw.group_names_by_key[transactions_raw.key] == "p1"


def test_transactions_dw_fail_toggle():
    result = materialize(
        P1,
        run_config={"ops": {"transactions_dw": {"config": {"fail": True}}}},
        raise_on_error=False,
    )
    assert not result.success
```

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p1_transactions.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines'`

- [ ] **Step 3: stub_config + P1 자산 구현**

`dagster/pipelines/__init__.py` = 빈 파일. `dagster/pipelines/stub_config.py`:
```python
from dagster import Config


class StubConfig(Config):
    """태스크 스텁의 실패 토글. 기본은 정상 동작."""
    fail: bool = False
```

`dagster/pipelines/p1_transactions/__init__.py` = 빈 파일. `dagster/pipelines/p1_transactions/assets.py`:
```python
from dagster import asset

from stubs import spark_task_stub
from pipelines.stub_config import StubConfig


@asset(group_name="p1", pool="spark")
def transactions_raw(config: StubConfig):
    """계좌 거래 원천 수집 (P1 T1)."""
    spark_task_stub("transactions_raw", duration_sec=10, fail=config.fail)


@asset(group_name="p1", deps=[transactions_raw], pool="spark")
def transactions_normalized():
    """거래 데이터 정규화 (P1 T2)."""
    spark_task_stub("transactions_normalized", duration_sec=5)


@asset(group_name="p1", deps=[transactions_normalized], pool="spark")
def transactions_dw(config: StubConfig):
    """DW 적재 (P1 T3). P2·P3 공통 상류, 시나리오 C 대상."""
    spark_task_stub("transactions_dw", duration_sec=8, fail=config.fail)
```

- [ ] **Step 4: 실험 파일 이동**

```bash
mkdir -p dagster/experiments && touch dagster/experiments/__init__.py
git mv dagster/priority_demo_job.py dagster/experiments/priority_demo_job.py
git mv dagster/cross_job_priority.py dagster/experiments/cross_job_priority.py
git rm -r dagster/p1_order_collection
```

`dagster/experiments/priority_demo_job.py`와 `cross_job_priority.py`의 stub 임포트 경로를 수정한다. 두 파일 상단의 `sys.path.insert(0, str(Path(__file__).parents[1] / "shared"))`를 `parents[2]`로 바꾼다(이제 `dagster/experiments/` 아래라 한 단계 깊어짐).

- [ ] **Step 5: definitions.py 갱신**

`dagster/definitions.py` 전체를 다음으로 교체:
```python
import sys
from pathlib import Path

# pipeline assets 가 'from stubs import' 하도록 shared/ 를 경로에 추가.
# (dagster/ 는 dagster 로더가 -f 로드 시 자동 추가하므로 pipelines.* 는 그대로 동작)
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))

from dagster import Definitions

from pipelines.p1_transactions.assets import (
    transactions_raw,
    transactions_normalized,
    transactions_dw,
)
from experiments.priority_demo_job import priority_demo_job
from experiments.cross_job_priority import cross_job_a, cross_job_b

defs = Definitions(
    assets=[transactions_raw, transactions_normalized, transactions_dw],
    jobs=[priority_demo_job, cross_job_a, cross_job_b],
)
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p1_transactions.py -v`
Expected: `3 passed`

- [ ] **Step 7: definitions 로드 확인**

Run: `source dagster/.venv/bin/activate && cd dagster && dagster definitions validate -f definitions.py`
Expected: `Validation successful` (또는 오류 없이 종료)

- [ ] **Step 8: Commit**

```bash
git add dagster/
git commit -m "feat: P1 은행 도메인 전환(transactions_*) + pipelines/ 재구성, 실험 experiments/ 이동

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: P2 일별 거래 집계 (Declarative Automation eager)

**Files:**
- Create: `dagster/pipelines/p2_aggregation/__init__.py` (빈), `dagster/pipelines/p2_aggregation/assets.py`
- Modify: `dagster/definitions.py`
- Create: `dagster/tests/test_p2_aggregation.py`

**Interfaces:**
- Consumes: `transactions_dw` (P1, group p1).
- Produces: 자산 `txn_loaded`, `daily_summary`, `summary_dw`, `summary_validated` (group `p2`). 전부 `automation_condition=AutomationCondition.eager()`. `summary_validated`는 `config: StubConfig`.

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_p2_aggregation.py`:
```python
from dagster import materialize

from pipelines.p1_transactions.assets import (
    transactions_raw,
    transactions_normalized,
    transactions_dw,
)
from pipelines.p2_aggregation.assets import (
    txn_loaded,
    daily_summary,
    summary_dw,
    summary_validated,
)

P1 = [transactions_raw, transactions_normalized, transactions_dw]
P2 = [txn_loaded, daily_summary, summary_dw, summary_validated]


def test_p2_chain_materializes_with_upstream():
    result = materialize(P1 + P2)
    assert result.success


def test_p2_has_eager_automation():
    # eager 조건이 부착되어 있는지 (값이 None 이 아니어야 함)
    assert txn_loaded.automation_conditions_by_key.get(txn_loaded.key) is not None


def test_p2_depends_on_transactions_dw():
    assert transactions_dw.key in txn_loaded.asset_deps[txn_loaded.key]
```

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p2_aggregation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines.p2_aggregation'`

- [ ] **Step 3: P2 자산 구현**

`dagster/pipelines/p2_aggregation/__init__.py` = 빈 파일. `dagster/pipelines/p2_aggregation/assets.py`:
```python
from dagster import asset, AutomationCondition

from stubs import spark_task_stub
from pipelines.stub_config import StubConfig
from pipelines.p1_transactions.assets import transactions_dw

EAGER = AutomationCondition.eager()


@asset(group_name="p2", deps=[transactions_dw], automation_condition=EAGER, pool="spark")
def txn_loaded():
    """거래 데이터 로드 (P2 T1)."""
    spark_task_stub("txn_loaded", duration_sec=5)


@asset(group_name="p2", deps=[txn_loaded], automation_condition=EAGER, pool="spark")
def daily_summary():
    """일별 집계 계산 (P2 T2)."""
    spark_task_stub("daily_summary", duration_sec=15)


@asset(group_name="p2", deps=[daily_summary], automation_condition=EAGER, pool="spark")
def summary_dw():
    """집계 결과 적재 (P2 T3)."""
    spark_task_stub("summary_dw", duration_sec=8)


@asset(group_name="p2", deps=[summary_dw], automation_condition=EAGER, pool="spark")
def summary_validated(config: StubConfig):
    """데이터 품질 검증 (P2 T4)."""
    spark_task_stub("summary_validated", duration_sec=5, fail=config.fail)
```

- [ ] **Step 4: definitions.py에 P2 등록**

`dagster/definitions.py`의 import 블록에 추가:
```python
from pipelines.p2_aggregation.assets import (
    txn_loaded,
    daily_summary,
    summary_dw,
    summary_validated,
)
```
`Definitions(assets=[...])`의 리스트에 `txn_loaded, daily_summary, summary_dw, summary_validated` 추가.

- [ ] **Step 5: 테스트 통과 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p2_aggregation.py -v`
Expected: `3 passed`

- [ ] **Step 6: Commit**

```bash
git add dagster/
git commit -m "feat: P2 일별 거래 집계 자산 + eager 자동화

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: P3 지급결제 정산 자산

P3 자산만 구현(센서는 Task 6). `settlement_base`는 `transactions_dw`에 의존(시나리오 C 차단 대상).

**Files:**
- Create: `dagster/pipelines/p3_settlement/__init__.py` (빈), `dagster/pipelines/p3_settlement/assets.py`
- Modify: `dagster/definitions.py`
- Create: `dagster/tests/test_p3_settlement.py`

**Interfaces:**
- Consumes: `transactions_dw` (P1).
- Produces: 자산 `settlement_base`, `fees_calculated`, `settlement_finalized`, `settlement_dw`, `settlement_file` (group `p3`).

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_p3_settlement.py`:
```python
from dagster import materialize

from pipelines.p1_transactions.assets import (
    transactions_raw,
    transactions_normalized,
    transactions_dw,
)
from pipelines.p3_settlement.assets import (
    settlement_base,
    fees_calculated,
    settlement_finalized,
    settlement_dw,
    settlement_file,
)

P1 = [transactions_raw, transactions_normalized, transactions_dw]
P3 = [settlement_base, fees_calculated, settlement_finalized, settlement_dw, settlement_file]


def test_p3_chain_materializes_with_upstream():
    result = materialize(P1 + P3)
    assert result.success


def test_p3_depends_on_transactions_dw():
    assert transactions_dw.key in settlement_base.asset_deps[settlement_base.key]
```

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p3_settlement.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines.p3_settlement'`

- [ ] **Step 3: P3 자산 구현**

`dagster/pipelines/p3_settlement/__init__.py` = 빈 파일. `dagster/pipelines/p3_settlement/assets.py`:
```python
from dagster import asset

from stubs import spark_task_stub
from pipelines.p1_transactions.assets import transactions_dw


@asset(group_name="p3", deps=[transactions_dw], pool="spark")
def settlement_base():
    """거래 집계 로드 (P3 T1). transactions_dw 의존 — 시나리오 C 차단 대상."""
    spark_task_stub("settlement_base", duration_sec=8)


@asset(group_name="p3", deps=[settlement_base], pool="spark")
def fees_calculated():
    """수수료 계산 (P3 T2)."""
    spark_task_stub("fees_calculated", duration_sec=20)


@asset(group_name="p3", deps=[fees_calculated], pool="spark")
def settlement_finalized():
    """정산 금액 확정 (P3 T3)."""
    spark_task_stub("settlement_finalized", duration_sec=10)


@asset(group_name="p3", deps=[settlement_finalized], pool="spark")
def settlement_dw():
    """정산 결과 적재 (P3 T4)."""
    spark_task_stub("settlement_dw", duration_sec=8)


@asset(group_name="p3", deps=[settlement_dw], pool="spark")
def settlement_file():
    """정산 파일(송금 파일) 생성 (P3 T5)."""
    spark_task_stub("settlement_file", duration_sec=5)
```

- [ ] **Step 4: definitions.py에 P3 등록**

import 블록에 추가:
```python
from pipelines.p3_settlement.assets import (
    settlement_base,
    fees_calculated,
    settlement_finalized,
    settlement_dw,
    settlement_file,
)
```
`Definitions(assets=[...])`에 5개 자산 추가.

- [ ] **Step 5: 테스트 통과 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p3_settlement.py -v`
Expected: `2 passed`

- [ ] **Step 6: Commit**

```bash
git add dagster/
git commit -m "feat: P3 지급결제 정산 자산 (transactions_dw 의존)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: P4 FDS 모델 학습 (graph_asset 2슬롯)

`fds_model`을 `@graph_asset`으로 — 내부 `pool="spark"` 병렬 학습 op 2개 → 2슬롯 점유.

**Files:**
- Create: `dagster/pipelines/p4_fds/__init__.py` (빈), `dagster/pipelines/p4_fds/assets.py`
- Modify: `dagster/definitions.py`
- Create: `dagster/tests/test_p4_fds.py`

**Interfaces:**
- Produces: 자산 `behavior_logs`, `fds_features`, `fds_model`(graph_asset), `fds_model_validated`, `fds_model_deployed` (group `p4`). 내부 op `fds_train_shard_a`, `fds_train_shard_b`(둘 다 `pool="spark"`), `fds_assemble`.

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_p4_fds.py`:
```python
from dagster import materialize

from pipelines.p4_fds.assets import (
    behavior_logs,
    fds_features,
    fds_model,
    fds_model_validated,
    fds_model_deployed,
)

P4 = [behavior_logs, fds_features, fds_model, fds_model_validated, fds_model_deployed]


def test_p4_chain_materializes():
    result = materialize(P4)
    assert result.success


def test_fds_model_is_graph_backed_with_two_spark_ops():
    # fds_model 내부 op 노드 중 pool="spark" 가 정확히 2개인지 (2슬롯 점유 보장)
    node_defs = fds_model.node_def.node_defs  # graph_asset 내부 그래프
    spark_ops = [n for n in node_defs if getattr(n, "pool", None) == "spark"]
    assert len(spark_ops) == 2
```

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p4_fds.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines.p4_fds'`

- [ ] **Step 3: P4 자산 구현**

`dagster/pipelines/p4_fds/__init__.py` = 빈 파일. `dagster/pipelines/p4_fds/assets.py`:
```python
from dagster import asset, graph_asset, op

from stubs import spark_task_stub


@asset(group_name="p4", pool="spark")
def behavior_logs():
    """거래 행동 로그 수집 (P4 T1)."""
    spark_task_stub("behavior_logs", duration_sec=15)


@asset(group_name="p4", deps=[behavior_logs], pool="spark")
def fds_features() -> str:
    """피처 엔지니어링 (P4 T2)."""
    spark_task_stub("fds_features", duration_sec=30)
    return "fds_features"


# --- P4 T3 모델 학습: 2슬롯 점유 (분산 학습 2 executor) ---
@op(pool="spark")
def fds_train_shard_a(features) -> str:
    spark_task_stub("fds_model_train_a", duration_sec=60)
    return "shard_a"


@op(pool="spark")
def fds_train_shard_b(features) -> str:
    spark_task_stub("fds_model_train_b", duration_sec=60)
    return "shard_b"


@op
def fds_assemble(a: str, b: str) -> str:
    spark_task_stub("fds_model_assemble", duration_sec=1)
    return "fds_model"


@graph_asset(group_name="p4")
def fds_model(fds_features: str) -> str:
    """모델 학습 (P4 T3). 내부 병렬 학습 op 2개로 spark 슬롯 2개 점유."""
    return fds_assemble(fds_train_shard_a(fds_features), fds_train_shard_b(fds_features))


@asset(group_name="p4", deps=[fds_model], pool="spark")
def fds_model_validated():
    """모델 검증 (P4 T4)."""
    spark_task_stub("fds_model_validated", duration_sec=15)


@asset(group_name="p4", deps=[fds_model_validated], pool="spark")
def fds_model_deployed():
    """모델 배포 (P4 T5)."""
    spark_task_stub("fds_model_deployed", duration_sec=5)
```

참고: `fds_model`은 `@graph_asset`이라 상류 의존을 **입력 인자**(`fds_features`)로 받는다(`@graph_asset`은 `deps=` 인자를 지원하지 않음). 스텁이라 인자 값을 실제로 쓰진 않지만, 인자로 받아야 `behavior_logs → fds_features → fds_model → validated → deployed` 계보가 한 줄로 이어진다.

- [ ] **Step 4: definitions.py에 P4 등록**

import 블록에 추가:
```python
from pipelines.p4_fds.assets import (
    behavior_logs,
    fds_features,
    fds_model,
    fds_model_validated,
    fds_model_deployed,
)
```
`Definitions(assets=[...])`에 5개 추가.

- [ ] **Step 5: 테스트 통과 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_p4_fds.py -v`
Expected: `2 passed`

- [ ] **Step 6: Commit**

```bash
git add dagster/
git commit -m "feat: P4 FDS 모델 학습 — fds_model graph_asset 2슬롯 점유

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: jobs / schedules / sensors + definitions 전체 배선

파이프라인별 asset job(우선순위 태그), P1·P4 스케줄, P3 AssetSensor, 자동화 센서를 만들고 definitions에 등록.

**Files:**
- Create: `dagster/jobs.py`, `dagster/schedules.py`, `dagster/sensors.py`
- Modify: `dagster/definitions.py`
- Create: `dagster/tests/test_orchestration.py`

**Interfaces:**
- Produces: `p1_job, p2_job, p3_job, p4_job`(asset jobs, priority 태그 2/1/3/0); `p1_schedule, p4_schedule`; `p3_settlement_sensor`(AssetSensor → p3_job); `automation_sensor`(eager 평가).

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_orchestration.py`:
```python
from jobs import p1_job, p2_job, p3_job, p4_job
from schedules import p1_schedule, p4_schedule


def test_priority_tags():
    assert p1_job.tags["dagster/priority"] == "2"
    assert p2_job.tags["dagster/priority"] == "1"
    assert p3_job.tags["dagster/priority"] == "3"
    assert p4_job.tags["dagster/priority"] == "0"


def test_schedule_crons():
    assert p1_schedule.cron_schedule == "0 * * * *"
    assert p4_schedule.cron_schedule == "0 2 * * 1"
```

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_orchestration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'jobs'`

- [ ] **Step 3: jobs.py 구현**

`dagster/jobs.py`:
```python
from dagster import define_asset_job, AssetSelection

# 파이프라인별 asset job. run 태그 dagster/priority 가 QueuedRunCoordinator 의 시작 순서를 정함.
p1_job = define_asset_job("p1_job", selection=AssetSelection.groups("p1"), tags={"dagster/priority": "2"})
p2_job = define_asset_job("p2_job", selection=AssetSelection.groups("p2"), tags={"dagster/priority": "1"})
p3_job = define_asset_job("p3_job", selection=AssetSelection.groups("p3"), tags={"dagster/priority": "3"})
p4_job = define_asset_job("p4_job", selection=AssetSelection.groups("p4"), tags={"dagster/priority": "0"})
```

- [ ] **Step 4: schedules.py 구현**

`dagster/schedules.py`:
```python
from dagster import ScheduleDefinition

from jobs import p1_job, p4_job

# P1: 매시간 정각, P4: 매주 월요일 02:00
p1_schedule = ScheduleDefinition(name="p1_hourly", job=p1_job, cron_schedule="0 * * * *")
p4_schedule = ScheduleDefinition(name="p4_weekly", job=p4_job, cron_schedule="0 2 * * 1")
```

- [ ] **Step 5: sensors.py 구현**

`dagster/sensors.py`:
```python
from dagster import (
    asset_sensor,
    AssetKey,
    RunRequest,
    AutomationConditionSensorDefinition,
    AssetSelection,
    DefaultSensorStatus,
)

from jobs import p3_job


# P3: transactions_dw 가 새로 materialize 되면 정산 job 트리거.
# (1일 1회 가드와 SLA 데드라인 탈출구는 4~7주차에 보강 — 골격은 이벤트 트리거만.)
@asset_sensor(
    asset_key=AssetKey("transactions_dw"),
    job=p3_job,
    default_status=DefaultSensorStatus.STOPPED,
)
def p3_settlement_sensor(context, asset_event):
    return RunRequest()


# P2 eager 자동화 평가용 센서 (daemon 이 실행해야 eager 가 발화).
automation_sensor = AutomationConditionSensorDefinition(
    "automation_sensor",
    target=AssetSelection.all(),
)
```

- [ ] **Step 6: definitions.py 전체 배선**

`dagster/definitions.py`의 `Definitions(...)`를 다음으로 확장(자산 import는 기존 유지):
```python
from jobs import p1_job, p2_job, p3_job, p4_job
from schedules import p1_schedule, p4_schedule
from sensors import p3_settlement_sensor, automation_sensor

defs = Definitions(
    assets=[
        transactions_raw, transactions_normalized, transactions_dw,
        txn_loaded, daily_summary, summary_dw, summary_validated,
        settlement_base, fees_calculated, settlement_finalized, settlement_dw, settlement_file,
        behavior_logs, fds_features, fds_model, fds_model_validated, fds_model_deployed,
    ],
    jobs=[p1_job, p2_job, p3_job, p4_job, priority_demo_job, cross_job_a, cross_job_b],
    schedules=[p1_schedule, p4_schedule],
    sensors=[p3_settlement_sensor, automation_sensor],
)
```

- [ ] **Step 7: 테스트 + 정의 검증**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_orchestration.py -v`
Expected: `2 passed`

Run: `cd dagster && dagster definitions validate -f definitions.py`
Expected: 오류 없이 종료 (`Validation successful`)

- [ ] **Step 8: Commit**

```bash
git add dagster/
git commit -m "feat: jobs(우선순위 태그)/schedules/sensors + definitions 전체 배선

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: dagster.yaml 동시성 설정

slot pool(limit 3, granularity op)과 run 큐 우선순위(QueuedRunCoordinator)를 인스턴스 설정에 반영.

**Files:**
- Modify: `dagster/.dagster_home/dagster.yaml`
- Create: `dagster/tests/test_instance_config.py`

**Interfaces:**
- Produces: spark 풀 limit=3, granularity=op, QueuedRunCoordinator(max_concurrent_runs=2) 활성.

- [ ] **Step 1: 실패 테스트 작성**

`dagster/tests/test_instance_config.py`:
```python
from pathlib import Path

import yaml

YAML = Path(__file__).parents[1] / ".dagster_home" / "dagster.yaml"


def test_pool_granularity_is_op():
    cfg = yaml.safe_load(YAML.read_text())
    assert cfg["concurrency"]["pools"]["granularity"] == "op"


def test_run_coordinator_is_queued():
    cfg = yaml.safe_load(YAML.read_text())
    assert cfg["run_coordinator"]["module"] == "dagster.core.run_coordinator"
    assert cfg["run_coordinator"]["class"] == "QueuedRunCoordinator"


def test_max_concurrent_runs_set():
    cfg = yaml.safe_load(YAML.read_text())
    assert cfg["concurrency"]["runs"]["max_concurrent_runs"] == 2


def test_instance_actually_loads():
    # 텍스트 파싱만으론 잡지 못하는 설정 충돌(예: max_concurrent_runs 위치 오류)을
    # 실제 인스턴스 로드로 검증한다.
    import os

    os.environ["DAGSTER_HOME"] = str(YAML.parent)
    from dagster import DagsterInstance

    rq = DagsterInstance.get().get_concurrency_config().run_queue_config
    assert rq.max_concurrent_runs == 2
```

(yaml 미설치 시: `pip install pyyaml` — dagster 의존성에 이미 포함되어 있을 가능성 높음. 없으면 requirements에 추가.)

- [ ] **Step 2: 실패 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_instance_config.py -v`
Expected: FAIL — `KeyError: 'granularity'` (또는 run_coordinator)

- [ ] **Step 3: dagster.yaml 수정**

`dagster/.dagster_home/dagster.yaml` 전체를 다음으로:
```yaml
# Dagster 인스턴스 설정. DAGSTER_HOME 이 이 디렉토리를 가리켜야 읽힌다.

concurrency:
  # [슬롯] spark 풀 동시 op 수 = 3 (명세서 Spark 슬롯 3개).
  # granularity=op: 슬롯을 op 단위로 점유 → 파이프라인별 슬롯 가중치(P4 T3=2) 표현 가능.
  pools:
    default_limit: 3
    granularity: op
  # [우선순위] 동시 실행 run 수 상한. 트리거된 run 이 이 값을 넘으면 초과분이 큐잉되어
  # dagster/priority 순으로 시작된다(시나리오 B 우선순위 소비 관측 전제).
  # 주의: pools 와 함께 쓸 때 max_concurrent_runs 는 반드시 여기(concurrency.runs)에 둔다.
  #       run_coordinator.config 에 두면 pools 와 충돌해 인스턴스 로드가 실패한다.
  runs:
    max_concurrent_runs: 2

# run 태그 dagster/priority 로 대기 run 의 시작 순서를 정렬 (Phase 1).
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator

# [deadlock 방지] run 취소/실패 시 슬롯 자동 반환 (default_limit 환경 보호).
run_monitoring:
  enabled: true
  free_slots_after_run_end_seconds: 60
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/test_instance_config.py -v`
Expected: `4 passed`

- [ ] **Step 5: 전체 테스트 + dagster dev 기동 확인**

Run: `source dagster/.venv/bin/activate && python -m pytest dagster/tests/ shared/tests/ -v`
Expected: 모든 테스트 PASS

Run (수동, 백그라운드): `cd dagster && DAGSTER_HOME=$(pwd)/.dagster_home dagster dev -f definitions.py`
Expected: http://localhost:3000 에서 4개 그룹(p1~p4) 자산 계보, jobs 4개, schedules 2개, sensors 2개 표시. 확인 후 종료.

- [ ] **Step 6: Commit**

```bash
git add dagster/
git commit -m "feat: dagster.yaml 동시성 설정 — spark pool limit=3 granularity=op + QueuedRunCoordinator

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: A/B/C 시나리오 사전 검증 (수동 + 검증 로그)

`dagster dev` 인스턴스에서 설계 §5 절차를 수행하고 관측 결과를 검증 로그로 남긴다. (우선순위/슬롯 경합은 in-process materialize로 재현 불가 → daemon 포함 UI에서 수동 관측.)

**Files:**
- Create: `docs/record/week3/2026-06-21-week3-scenario-verification.md`

**전제:**
- `cd dagster && DAGSTER_HOME=$(pwd)/.dagster_home dagster dev -f definitions.py` 기동.
- UI에서 **`automation_sensor`(P2 eager 발화용)** 와 **`p3_settlement_sensor`(P3 트리거용)** 를 모두 **ON** (둘 다 기본 STOPPED). daemon이 떠 있어야 eager·센서가 평가된다.
- 슬롯 경합은 각 자산 duration이 길어 자연 관측 가능(P4 학습 60s×2). 필요 시 launchpad에서 추가 트리거.

- [ ] **Step 1: 시나리오 C 검증 (의존성 + 선택적 재실행)**

1. UI에서 P1 그룹 materialize, 단 launchpad run config에 `{"ops": {"transactions_dw": {"config": {"fail": true}}}}` 입력 → `transactions_dw` 실패 확인.
2. P2(eager)·P3(sensor) 가 발화하지 않음(차단) 확인. (P3 센서는 UI에서 ON 필요)
3. `transactions_dw` 자산만 선택 → Re-materialize (fail 미설정) → 성공. `transactions_raw`/`transactions_normalized`는 재실행 안 됨 확인.
4. eager로 P2 자동 materialize, 센서로 P3 자동 트리거 확인.
- Expected: T1·T2 재실행 없이 dw만 재실행, P2·P3 자동 unblock.

- [ ] **Step 2: 시나리오 A 검증 (우선순위 선점)**

1. P4 job 트리거 → `fds_model` 단계에서 spark 슬롯 2/3 점유 확인(UI Concurrency 뷰).
2. P3 job 수동 트리거(priority=3).
- Expected: P3 가 여분 슬롯 1개로 즉시 시작(P4 완료를 기다리지 않음). **풀 만석(P4 2 + P3 1 = 3) saturation 은 이 시나리오에서 관측된다.** *op 단위 끼어들기는 Phase 1 미지원 — 한계로 기록.*
- 참고(minor): `RunQueueConfig` 기본값 `should_block_op_concurrency_limited_runs=True` 라 풀 슬롯이 부족한 run 의 admit 이 블록될 수 있다. P3 가 여분 슬롯이 있는데도 즉시 admit 되지 않으면 이 옵션을 점검한다.

- [ ] **Step 3: 시나리오 B 검증 (동시성 제어)**

1. P1·P3·P4 job 을 동시 트리거(트리거 run 수 3 > max_concurrent_runs 2 → 초과분 큐잉).
2. P2 도 경합에 포함하려면 **`p2_job` 을 수동 트리거**한다(우선순위 1 보장). *주의: P2 가 eager 자동으로 뜬 run 은 `p2_job` 태그를 상속하지 않아 기본 우선순위로 admit되므로, B의 우선순위 서열 관측 시엔 수동 트리거를 쓴다.*
- Expected: 큐잉된 run 이 `dagster/priority` 순으로 시작(P4 최하위 뒤로 밀림) = **run 큐 우선순위가 이 시나리오에서 관측된다.**
- 참고(층위 구분): `max_concurrent_runs=2` 면 P4 가 run 레벨에서 큐잉돼 P4 의 2슬롯이 풀에 도달하지 않으므로, B 에서 실제 동시 슬롯은 P1+P3=2 까지다. 즉 **풀 limit-3 saturation 은 시나리오 A 에서, run 큐 우선순위는 B 에서** 관측된다("B 에서 동시 op ≤ 3"으로 적지 말 것 — 오해 소지).
- 참고: 큐 우선순위를 더 또렷이 보려면 `max_concurrent_runs` 를 1로 낮춰 재기동. 슬롯(op) 단위 우선순위 소비는 Phase 2(Celery) — 한계로 기록.

- [ ] **Step 4: 검증 로그 작성**

`docs/record/week3/2026-06-21-week3-scenario-verification.md`에 각 시나리오: 수행 절차 / 관측된 동작(스크린샷 또는 run 로그 요약) / 합격 여부 / Phase 1 한계 기록. 설계 §5 합격 기준과 대조. 특히 **"풀 saturation은 A, run 큐 우선순위는 B"** 의 층위 구분을 명시한다.

- [ ] **Step 5: Commit**

```bash
git add docs/record/week3/2026-06-21-week3-scenario-verification.md
git commit -m "docs: 3주차 A/B/C 시나리오 사전 검증 로그

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review (작성자 체크)

**Spec coverage (설계 문서 대조):**
- §2 자산 계보(17 자산) → Task 2(P1)·3(P2)·4(P3)·5(P4) ✓
- §2-3 duration/fail → 각 자산 본문 + StubConfig(raw/dw/validated fail) ✓
- §3 구동 레이어(트리거/우선순위/스케줄) → Task 6 jobs/schedules/sensors ✓
- §3-3 P3 SLA 데드라인 탈출구 → 골격은 기본 센서, 탈출구는 4~7주차(Task 6 주석에 명시) ✓
- §4 슬롯(pool limit3 granularity op) → **모든 spark 태스크 자산에 `pool="spark"`**(Task 2~5) + P4 graph_asset 2슬롯(Task 5) + dagster.yaml(Task 7) ✓
- §4 run 큐 우선순위 → Task 6 태그 + Task 7 QueuedRunCoordinator(max_concurrent_runs=2) ✓
- §5 시나리오 A/B/C 검증 → Task 8 ✓
- §6 코드 구조 → Task 2~7 파일 배치 ✓

**API 정확성(dagster 1.13.9 venv 실측):** `@asset(pool="spark")` → op.pool="spark"; graph_asset 내부는 `fds_model.node_def.node_defs`로 접근(`.op`는 불가); graph_asset 상류 의존은 입력 인자로 성립(`deps=` 미지원); asset config fail 토글은 `run_config={"ops":{...}}`; `define_asset_job(tags=...)`, `asset_sensor`, `AutomationConditionSensorDefinition`, `max_concurrent_runs` config 모두 동작 확인.

**Placeholder scan:** 모든 코드 스텝에 실제 코드. "적절히 처리" 류 없음.

**Type consistency:** 자산/op 이름이 import·test·definitions 전반에서 일치(`transactions_*`, `txn_loaded`/`daily_summary`/`summary_dw`/`summary_validated`, `settlement_*`, `fds_*`, job `p{n}_job`, `dagster/priority` 문자열 값).

**주의 (시나리오 검증 시):**
- `automation_sensor`·`p3_settlement_sensor`는 기본 STOPPED — UI에서 ON 필요(Task 8 전제).
- P2 eager 자동 run 은 `p2_job` 우선순위 태그를 상속하지 않음 → 시나리오 B 서열 관측 시 `p2_job` 수동 트리거(Task 8 Step 3).
- Phase 1은 run 큐 우선순위까지만. op(슬롯) 단위 우선순위 소비는 Phase 2(Celery) — Task 8에서 한계로 기록.

**의도적 후속:** P3 1일1회 가드 + SLA 데드라인 탈출구(2×2 비교의 P3-eager 포함)는 4~7주차. 본 골격은 기본 트리거까지.
