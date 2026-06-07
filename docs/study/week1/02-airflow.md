# 블록 2 — Airflow 아키텍처

> 참조: [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
> 블록 1 핵심 개념을 Airflow가 구체적으로 어떻게 구현하는지를 정리한다.

---

## 2.1 전체 아키텍처

Airflow는 6개 컴포넌트로 구성된다.

```
┌──────────────────────────────────────────────────┐
│                   DAG Directory                  │
│  (파이썬 파일들 — DAG 정의가 담긴 폴더)              │
└──────────────┬───────────────────────────────────┘
               │ 읽음 (파싱)
               ▼
┌─────────────────────────┐       ┌──────────────────┐
│       Scheduler          │◄─────►│   Metadata DB    │
│  (판단·선별·트리거)        │       │  (모든 상태 저장)  │
└──────────┬──────────────┘       └────────┬─────────┘
           │ Task 전달                      │ 읽기
           ▼                               ▼
┌──────────────────────┐        ┌──────────────────────┐
│       Executor        │        │      Webserver        │
│  (실행 방식 결정)      │        │  (UI — 감시·트리거)    │
└──────────┬───────────┘        └──────────────────────┘
           │
           ▼
┌──────────────────────┐
│       Worker(s)       │
│  (실제 코드 실행)      │
└──────────────────────┘
```

| 컴포넌트 | 역할 |
|---|---|
| **Scheduler** | 스케줄된 워크플로우 트리거 + 실행 가능 Task를 Executor에 제출 |
| **Executor** | Scheduler 내 내장 설정. Task를 어떤 방식으로·어디서 실행할지 결정 |
| **Worker** | Executor가 배정한 Task를 실제로 실행하는 프로세스 |
| **Webserver** | DAG·Task 상태 조회·트리거·디버그를 위한 UI |
| **Metadata DB** | Task 상태·DAG·변수 등 모든 상태의 단일 진실 원천 (PostgreSQL/MySQL) |
| **DAG Directory** | Scheduler가 파싱하는 파이썬 파일 폴더 |

---

## 2.2 DAG 작성 방식 (Python)

Airflow DAG는 **파이썬 파일**로 정의한다. 세 가지 작성 방식 중 Context Manager가 가장 흔하다.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="p1_order_collection",
    start_date=datetime(2026, 6, 7),   # 배포 직전 날짜로 설정
    schedule="0 * * * *",             # 매시간 정각 (cron)
    catchup=False,                     # 과거 누락 Run 소급 실행 방지
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    t1 = PythonOperator(task_id="collect_orders", python_callable=collect_orders_fn)
    t2 = PythonOperator(task_id="normalize_data",  python_callable=normalize_fn)
    t3 = PythonOperator(task_id="load_to_dw",     python_callable=load_dw_fn)

    t1 >> t2 >> t3
```

### 핵심 파라미터

| 파라미터 | 설명 | 과제 적용 |
|---|---|---|
| `dag_id` | DAG 고유 식별자 | `"p1_order_collection"` 등 |
| `schedule` | cron 표현식 또는 alias | P1 `"0 * * * *"`, P3 `"0 0 * * *"`, P4 `"0 2 * * 1"` |
| `start_date` | 스케줄 시작 시점 | 배포 직전 날짜 권장 |
| `catchup` | 과거 누락 Run 소급 여부 | **반드시 `False`** (아래 참조) |
| `default_args` | 모든 Task에 공통 적용 기본값 | retries, retry_delay 설정 |

### `catchup=False`가 필요한 이유

Airflow는 DAG Run을 "특정 **데이터 구간(data interval)**을 처리하는 실행"으로 설계했다. `start_date`가 과거면 Airflow는 "그 시점부터 지금까지 데이터가 처리 안 됐다"고 판단하고, `catchup=True`(기본값)이면 누락된 모든 구간의 Run을 한꺼번에 대기열에 넣는다.

예: `start_date=2026-01-01` + 오늘 배포 + P1(매시간) → 약 3,700개 Run이 동시에 생성됨.

**해결책**: `catchup=False` + `start_date`를 배포 직전 날짜로 설정.

### 의존성 선언 방법

```python
t1 >> t2 >> t3          # 선형
t1 >> [t2, t3] >> t4    # 분기 후 합류 (t2·t3 병렬 → 둘 완료 후 t4)
t3 << t2 << t1          # 역방향 (위와 동일 의미)
```

---

## 2.3 Task 상태 전이

```
none (의존성 미충족)
  │ 의존성 충족
  ▼
scheduled (Scheduler가 실행 가능 확인)
  │ Executor에 전달
  ▼
queued (워커 배정 대기 — Pool 슬롯 부족 시 여기서 대기)
  │ 워커 픽업
  ▼
running (실제 코드 실행 중)
  ├─ 성공 → success
  └─ 실패 → up_for_retry ─(재시도 소진)→ failed
                                              │
                                              ▼ (하류에 전파)
                                       upstream_failed
```

| 상태 | 의미 |
|---|---|
| `none` | 상류 Task가 아직 끝나지 않음 |
| `scheduled` | 의존성 충족, 실행 예정 |
| `queued` | Executor에 전달됨. Pool 슬롯 부족 시 여기서 대기 + 우선순위 정렬 |
| `running` | 워커가 실행 중 |
| `success` | 정상 완료 |
| `failed` | 재시도 소진 후 최종 실패 |
| `up_for_retry` | 실패했으나 재시도 횟수 남음 |
| `upstream_failed` | 내가 실패한 게 아니라 선행 Task가 실패해 실행 기회를 못 받은 상태 |
| `skipped` | 브랜치 로직 등으로 건너뜀 |

**`failed` vs `upstream_failed` 차이**: `failed`는 해당 Task 자신이 실패한 것. `upstream_failed`는 선행 Task 실패로 실행 기회조차 없는 것. 시나리오 C에서 P1 T3가 `failed`가 되면 P2·P3 전체가 `upstream_failed` 상태가 된다.

---

## 2.4 Pool — 동시성·우선순위 제어

공식 문서 정의: **임의 Task 집합의 동시 실행 병렬성을 제한하는 메커니즘.** Admin > Pools에서 이름과 슬롯 수를 설정.

### 슬롯 동작 원리

1. Pool의 슬롯 수 = 동시 실행 가능한 Task 수
2. 실행 중 Task가 슬롯 점유 → 슬롯이 다 차면 추가 Task는 `queued`에서 대기
3. 슬롯 반납 시 → 대기 중 Task를 `priority_weight` 기준으로 정렬 → 높은 것부터 슬롯 확보

### Task에 Pool 할당

```python
t3 = PythonOperator(
    task_id="load_to_dw",
    python_callable=load_dw_fn,
    pool="spark_pool",     # 이 Task는 spark_pool 슬롯 소비
    pool_slots=1,          # 기본값 1. 리소스 집약 Task는 2 이상 설정 가능
    priority_weight=2,     # 슬롯 경합 시 정렬 기준
)
```

### 과제 Pool 설계

```
Pool: spark_pool (slots=3)

P1 T1~T3   pool_slots=1, priority_weight=2
P2 T1~T4   pool_slots=1, priority_weight=1
P3 T1~T5   pool_slots=1, priority_weight=3   ← 최고 우선순위
P4 T3      pool_slots=2, priority_weight=0   ← 슬롯 2개 + 최저 우선순위
```

P4 T3에 `pool_slots=2`를 설정하는 이유: 모델 학습의 리소스 집약성을 논리적으로 표현. 슬롯 3개 중 2개가 P4에 묶이는 상황을 만들어, P3 긴급 트리거 시 우선순위 선점 시나리오를 연출한다.

---

## 2.5 Executor 종류

| 분류 | 대표 Executor | 동작 | 적합 환경 |
|---|---|---|---|
| **Local** | LocalExecutor | Scheduler 프로세스 내 서브프로세스 실행 | 단일 머신, 개발 환경 |
| **Remote/Queue** | CeleryExecutor | 메시지 브로커(Redis)에 Task 투입 → 별도 워커 실행 | 멀티 머신, 표준 운영 |
| **Remote/Container** | KubernetesExecutor | Task마다 파드 생성 → 완전 격리 | Cloud-native |

**과제 셋업**: Docker Compose 기반 CeleryExecutor 구성이 표준 (Postgres + Redis + Worker).

---

## 2.6 ExternalTaskSensor — 크로스 DAG 의존성

블록 1의 "이벤트/센서 트리거"의 Airflow 구체 구현.

```python
from airflow.sensors.external_task import ExternalTaskSensor

# P2 DAG 안에서 P1 완료를 기다리는 방법
wait_for_p1 = ExternalTaskSensor(
    task_id="wait_for_p1",
    external_dag_id="p1_order_collection",
    external_task_id="load_to_dw",   # P1의 T3(DW 적재) 완료 감지
    mode="reschedule",               # 주기적으로 확인 (슬롯 점유 최소화)
)

wait_for_p1 >> p2_t1 >> p2_t2 >> ...
```

**시나리오 C 연결**: P1 T3(`load_to_dw`)가 `failed`가 되면 Sensor가 영구 대기 또는 타임아웃으로 실패 → P2 전체 `upstream_failed`. T3를 선택적 재실행해 `success`로 전환하면 Sensor가 감지 → P2 자동 재개.

---

## 2.7 과제 파이프라인 매핑 요약

| 시나리오 | Airflow 구현 요소 |
|---|---|
| **A (우선순위 선점)** | `spark_pool` + `priority_weight` + P4 T3의 `pool_slots=2` |
| **B (동시성 제어)** | `spark_pool slots=3` + 대기열의 `priority_weight` 정렬 |
| **C (의존성 + 선택적 재실행)** | `ExternalTaskSensor` + UI에서 특정 Task만 Clear & Re-run |

cron 스케줄 요약:

| 파이프라인 | schedule |
|---|---|
| P1 (매시간 정각) | `"0 * * * *"` |
| P2 (P1 완료 후) | `ExternalTaskSensor`로 P1 감지 |
| P3 (자정 + 긴급) | `"0 0 * * *"` + 수동 API 트리거 |
| P4 (매주 월 02:00) | `"0 2 * * 1"` |
