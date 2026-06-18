# 블록 3 — Dagster 아키텍처

> 참조: [Dagster 공식 문서](https://docs.dagster.io/)

---

## 3.1 핵심 패러다임 — "만들 것"을 선언한다

Dagster는 **데이터 결과물(Asset)**을 중심에 놓는다. 시스템을 바라보는 질문이 "파이프라인이 제대로 돌았는가"가 아니라 **"데이터가 최신 상태인가"**다.

```
orders_raw ──→ orders_normalized ──→ orders_dw
```

의존성을 표현할 때 실행 순서를 지정하는 게 아니라, **데이터 자산 간 관계(lineage)를 선언**한다. 실행 순서는 Dagster가 그 관계에서 추론한다. 이 lineage는 런타임에 데이터 흐름을 감시해 만드는 게 아니라 **정의를 로드하는 시점에 선언 구조로 정적 구성**되므로, 실행 전에 이미 전체 계보가 그려진다.

Dagster UI(Dagit)의 핵심 화면이 **Asset Catalog**인 이유가 여기 있다 — 운영자가 "이 테이블이 언제 마지막으로 갱신됐는가"를 기준으로 시스템을 모니터링한다.

---

## 3.2 컴포넌트 구조

```
┌─────────────────────────────────────────────────┐
│              Code Location (서버)                 │
│  (파이썬 코드 — Asset/Op/Job/Schedule/Sensor 정의) │
└──────────────┬──────────────────────────────────┘
               │ 코드 로드
               ▼
┌──────────────────────┐      ┌─────────────────────┐
│   Dagster Daemon      │◄────►│   Metadata DB        │
│  (스케줄·센서 틱 실행, │      │  (Run 상태·Asset 상태 │
│   Run 모니터링)        │      │   이력 저장)          │
└──────────┬───────────┘      └──────────┬──────────┘
           │ Run 시작                     │ 읽기
           ▼                             ▼
┌──────────────────────┐     ┌───────────────────────┐
│    Run Launcher       │     │   Webserver (Dagit)    │
│  (어디서 실행할지 결정)│     │  (Asset 계보·Run 조회, │
└──────────┬───────────┘     │   수동 트리거)          │
           ▼                 └───────────────────────┘
┌──────────────────────┐
│      Executor         │
│  (Op/Asset 실행 방식) │
└──────────────────────┘
```

| 컴포넌트 | 역할 |
|---|---|
| **Webserver (Dagit)** | Asset 계보·Run 상태 UI. `dg dev` 한 줄로 실행 가능 |
| **Daemon** | 스케줄·센서 틱 실행, Run 상태 모니터링 |
| **Run Launcher** | Run 전체를 담을 프로세스를 어디에 띄울지 결정 (로컬 프로세스 / 컨테이너 등) |
| **Executor** | Run 내 Op·Asset들을 어떻게 실행할지 결정 (순차 / 병렬) |
| **Code Location** | Asset·Op·Job·Schedule·Sensor 정의가 담긴 파이썬 코드 서버 |
| **Metadata DB** | Run 이력·Asset 구체화 상태 저장 |

**Run Launcher vs Executor**: Run Launcher는 "Run 전체를 어디서 돌릴까" (프로세스를 어느 머신·컨테이너에 띄울지), Executor는 "그 Run 안에서 Op들을 어떻게 실행할까" (순차냐 병렬이냐). 결정하는 레이어가 다르다.

**Daemon이 없으면**: UI에서 수동 materialize는 되지만 **스케줄·센서·Run 큐가 전부 작동하지 않는다** → 자동 파이프라인(P3 자정, P2의 P1 완료 감지 등) 정지. `dg dev`가 Webserver와 Daemon을 함께 띄우는 이유다.

---

## 3.3 Asset — 코드 작성 방식

공식 문서 기준 `@dg.asset` 데코레이터로 정의한다.

```python
import dagster as dg

@dg.asset
def orders_raw():
    # T1: 주문 원천 수집
    spark = SparkSession.builder.appName("orders_raw").master("local[*]").getOrCreate()
    time.sleep(10)
    spark.stop()

@dg.asset(deps=[orders_raw])           # orders_raw 완료 후 실행 — 이름으로 선언
def orders_normalized(context: dg.AssetExecutionContext):
    context.log.info("normalizing...")
    time.sleep(5)

@dg.asset(deps=[orders_normalized])
def orders_dw():
    time.sleep(8)
```

**의존성 표현**: `deps=[upstream_asset]` 파라미터로 **어떤 데이터 자산에 의존하는지 선언**. 실행 순서는 Dagster가 추론.

**의존성 선언 두 방식 — `deps` vs 파라미터**

자산 의존성은 두 가지로 선언할 수 있고, *데이터 값을 주입받느냐*에서 갈린다.

| 방식 | 선언 | 데이터 값 | 용도 |
|---|---|---|---|
| **`deps`** | `@asset(deps=[upstream])` | 주입 안 됨 (의존성만) | 하류가 저장소에서 직접 읽거나 값 전달이 불필요할 때 |
| **파라미터** | `def x(upstream)` | 상류 반환값이 **주입**되어 로직에서 사용 | 데이터를 함수 반환값으로 주고받을 때 |

```python
@dg.asset
def orders_normalized(orders_raw):   # 파라미터로 받으면 orders_raw '값'이 주입됨
    return orders_raw.dropna()
```

- **과제는 `deps` 방식**: 태스크가 `time.sleep` stub이고 Spark가 DW에 직접 적재·조회하므로 값 전달이 불필요.
- 두 방식 모두 의존성을 선언하므로 **lineage·실행 순서는 동일**. 차이는 값 주입 여부뿐.
- 파라미터 방식에서 값을 실제 저장·로드하는 건 **IO Manager**가 담당(기본 로컬 직렬화, 설정 시 Parquet·DW 테이블 등). `deps`는 값 전달이 없어 관여하지 않음.

**데코레이터 4종류:**

| 데코레이터 | 용도 |
|---|---|
| `@asset` | 단일 자산 |
| `@multi_asset` | 하나의 함수에서 여러 자산 동시 생성 |
| `@graph_asset` | 복수 Op → 단일 자산 |
| `@graph_multi_asset` | 복수 Op → 여러 자산 |

---

## 3.4 Op & Job — Asset 이전 모델

공식 문서: "지금 시작하는 사람은 Asset을 쓰길 강력 권장." Op은 레거시 워크플로우용으로 남아 있지만, Dagster 내부 동작 이해와 과제의 PySpark 직접 호출 구현을 위해 기본은 알아야 한다.

```python
import dagster as dg

@dg.op
def collect_orders(context: dg.OpExecutionContext):
    context.log.info("collecting...")
    time.sleep(10)
    return "raw_data"

@dg.op
def normalize_data(context, raw_data: str):  # 상류 Op의 반환값을 인자로 받음
    time.sleep(5)
    return "normalized_data"

@dg.op
def load_to_dw(context, normalized_data: str):
    time.sleep(8)

@dg.job
def p1_pipeline():
    load_to_dw(normalize_data(collect_orders()))  # 함수 호출 체인으로 의존성 표현
```

**Asset vs Op 의존성 표현 차이:**

| | Asset | Op |
|---|---|---|
| 의존성 문법 | `deps=[orders_raw]` — 이름 선언 | `load_to_dw(normalize_data(...))` — 함수 호출 체인 |
| 데이터 흐름 | 데이터 자산(테이블·파일) | 상류 Op의 반환값 → 하류 Op의 인자 |
| UI 표현 | Asset Catalog (계보 시각화) | Job/Graph 실행 그래프 |
| 재실행 단위 | Asset 단위 re-materialization | Step(Op) 단위 re-execution |

---

## 3.5 스케줄 & 센서

### 스케줄

```python
import dagster as dg

p1_schedule = dg.ScheduleDefinition(
    name="p1_hourly",
    cron_schedule="0 * * * *",                               # 매시간 정각
    target=[orders_raw, orders_normalized, orders_dw],       # 실행할 Asset 지정
)

p3_schedule = dg.ScheduleDefinition(
    name="p3_daily_midnight",
    cron_schedule="0 0 * * *",
    target=[settlement_job],
)
```

### 센서 — 이벤트 기반 트리거

Daemon이 `minimum_interval_seconds` 주기로 폴링해서 조건 충족 시 Run을 시작시킨다.

```python
@dg.sensor(job=p2_pipeline, minimum_interval_seconds=30)
def p1_completion_sensor(context):
    if p1_orders_dw_materialized():
        yield dg.RunRequest(run_key="p2_trigger")
    else:
        yield dg.SkipReason("P1 not yet complete")
```

**`run_key`**: 동일 이벤트로 Run이 중복 생성되는 것을 방지. 이미 처리한 `run_key`는 Dagster가 자동 무시.

### Asset Sensor — 크로스 파이프라인 의존성

```python
@dg.asset_sensor(asset_key=dg.AssetKey("orders_dw"), job=p2_pipeline)
def p2_trigger_sensor(context, asset_event):
    yield dg.RunRequest()
```

`orders_dw`라는 **데이터 자산이 생성됐는지**만 감시한다. 그게 어떤 Job에서 어떤 Op을 통해 만들어졌는지는 관심 없다. 데이터 계보(lineage)가 연결 고리를 담당하기 때문에, 상류 파이프라인의 구조 변경에 영향을 받지 않는다.

**시나리오 C 연결**: P1 T3(`orders_dw` 생성)가 실패하면 Asset Sensor가 트리거되지 않아 P2 Run이 시작되지 않는다. T3를 re-materialize해 성공하면 Sensor가 감지 → P2 자동 실행.

---

## 3.6 동시성 제어

Dagster의 동시성 제어는 **두 레벨**이다. 단, 둘 다 작동하려면 **Run Coordinator가 `QueuedRunCoordinator`** 여야 한다.

**Run Coordinator** — Run 제출 시 즉시 시작할지 큐에 넣을지 결정.
- `DefaultRunCoordinator`: 제출 즉시 시작 (한도·우선순위 없음)
- `QueuedRunCoordinator`: Run을 큐에 넣어 한도·우선순위 적용 → **동시성·우선순위 제어의 전제**. 큐 소비는 Daemon이 담당.

### 레벨 1 — Run 레벨

동시에 실행될 수 있는 Run의 수 제한. `dagster.yaml`에서 설정.

```yaml
# dagster.yaml
concurrency:
  default_op_concurrency_limit: 3
```

### 레벨 2 — Op/Asset 레벨 (Concurrency Key)

특정 태그를 가진 Op들 간의 전역 동시 실행 수 제한. 과제의 "Spark 슬롯 3개" 제약을 구현하는 방법.

```python
@dg.op(tags={"dagster/concurrency_key": "spark_slot"})
def load_to_dw(context):
    time.sleep(8)
```

```yaml
# dagster.yaml
concurrency:
  pools:
    - pool_name: spark_slot
      slots: 3          # 동시에 spark_slot 태그 Op 최대 3개
```

태그 기반으로 선언한다. 우선순위는 Job/Run 레벨에서 `priority` 파라미터로 설정한다.

**granularity 주의**: Dagster에서 **우선순위는 Run 수준**(큐에서 Run을 꺼내는 순서), **슬롯 제한은 스텝(op/asset) 수준**(Concurrency Key)이라 두 제어가 노는 층위가 다르다. 과제처럼 *태스크 단위* 우선순위 선점(시나리오 A)을 정밀히 재현할 땐 이 층위 차이를 고려해야 한다.

---

## 3.7 선택적 재실행 — 시나리오 C 핵심

Dagster의 재실행 단위는 **Asset** (또는 Op 모델에서는 Step).

- **Asset 모델**: UI에서 `orders_dw` Asset만 선택해 re-materialization. 상류 자산은 건드리지 않음.
- **Op 모델**: UI에서 특정 Step만 선택해 re-execution. "From failure" 옵션으로 실패 지점부터 재실행도 가능.

Asset 모델이 UI에서 더 직관적이며, 재실행 대상을 "어떤 데이터가 최신이 아닌가"로 직접 선택할 수 있다.

---

## 3.8 과제 파이프라인 매핑 요약

| 시나리오 | Dagster 구현 요소 |
|---|---|
| **A (우선순위 선점)** | `dagster/concurrency_key` 태그 + `priority` 설정 |
| **B (동시성 제어)** | Concurrency Key pool `slots=3` |
| **C (의존성 + 선택적 재실행)** | `AssetSensor` + Asset 단위 re-materialization |

cron 스케줄 요약:

| 파이프라인 | cron_schedule |
|---|---|
| P1 (매시간 정각) | `"0 * * * *"` |
| P2 (P1 완료 후) | `@asset_sensor(asset_key=AssetKey("orders_dw"))` |
| P3 (자정 + 긴급) | `"0 0 * * *"` + 수동 UI/API 트리거 |
| P4 (매주 월 02:00) | `"0 2 * * 1"` |
