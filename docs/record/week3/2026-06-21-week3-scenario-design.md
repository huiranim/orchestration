# 3주차 설계 — A/B/C 시나리오 재현을 위한 파이프라인·환경 설계 (은행 / Dagster)

> 브레인스토밍 결과 기록 (2026-06-21)
> 명세서 §5 3주차 일정 기반: 시나리오 설계 + 환경 구성. 4개 파이프라인 DAG 설계(3h) + 환경 셋업·스텁·Spark 연동(2h) + 골격 구현(3h) + A/B/C 사전 검증(2h)
> 전제: 2주차 도구 선택 결정(`docs/study/week2/05-비교분석.md`)에 따라 **Dagster로 진행**.

> **이 설계의 목적은 시나리오 A/B/C의 재현·검증이다.** 파이프라인·슬롯·트리거·환경은 모두 "이 셋을 재현하려면 무엇이 필요한가"에서 거꾸로 도출된 수단이다. 명세서 §4-3이 못박듯, *"구현의 정합성은 이 세 시나리오를 재현·검증할 수 있는지로 판단한다."* 따라서 §5가 이 문서의 핵심이고, §2~4는 그 무대 장치다.

---

## 0. 명세서 대비 변경·확장 요약 (먼저 읽을 것)

이 설계는 명세서(`docs/specs/2026-06-04-orchestration-homework-design.md`)를 기반으로 하되, 아래 두 가지를 **의도적으로 변경·확장**했다. 충돌 시 명세서가 SoT이나, 본 항목은 브레인스토밍에서 합의한 의식적 결정이다.

| 구분 | 명세서 | 본 설계 | 근거 |
|---|---|---|---|
| **도메인 변경** | 이커머스(주문·매출·정산·추천) | **은행 데이터 플랫폼**(거래수집·거래집계·지급결제정산·FDS) | 구조(주기·우선순위·슬롯·태스크 수·의존성) 전부 동일하게 유지하고 도메인 의미만 치환. 정산·FDS가 은행 핵심 업무라 더 자연스러움. **시나리오 A/B/C 설계 불변** |
| **시나리오 C 확장** | P1 T3 실패는 P2·P3 차단 | P2는 하드 차단, **P3는 "SLA 데드라인 탈출구" 추가** | 최고 우선순위 P3(정산)의 SLA(06:00) 보장. 평소엔 의존(C 보존), 컷오프 초과 시 강제 실행. §5-C 참조 |

---

## 1. 핵심 결정 사항

각 결정이 **어느 시나리오를 위한 것인지**를 마지막 열에 표시한다 (수단이 목적을 따른다).

| 항목 | 결정 | 근거 | 위한 시나리오 |
|---|---|---|---|
| 충실도 전략 | **단계적**: Phase 1(run 레벨, 3주차) → Phase 2(Celery op 레벨, 4~5주차) | op 단위 크로스 run 우선순위는 Celery 필요(2주차 §1-6/1-7). 코드는 executor-agnostic이라 전환은 설정/인프라 변경 | **A, B** |
| 자산 입도 | **1 태스크 = 1 자산** | 자산 단위 재실행/우선순위 | **C** (+A/B의 op 단위) |
| 자산 이름 | **데이터 중심**(행위 아님) | Dagster 사상 | 전반 |
| 크로스 파이프라인 트리거 | **㉠ Declarative Automation 주력 + ㉡ AssetSensor 비교** | "자동 unblock"이 선언으로 떨어짐(㉠). ㉡은 8주차 비교용 | **C** |
| 슬롯 메커니즘 | **Concurrency pool** `spark`, limit=3, **granularity=op** | 명세서 "Spark 슬롯 3개" = cross-run op 동시성. granularity=op라야 슬롯 가중치(1/1/1/2) 표현 | **A, B** |
| Phase 1 우선순위 | **run 태그 `dagster/priority` + QueuedRunCoordinator** | run 큐 시작 순서 정렬 = "굵은 단위" 우선순위 | **A, B** |
| P4 T3 2슬롯 | **`@graph_asset` 내부 병렬 op 2개** | 슬롯은 op 단위 점유(가중치 없음) → 2슬롯 = 2 op-step | **A** |
| fail 시뮬레이션 | **코드 수정이 아니라 run config/tag로 토글** | 반복 검증 용이(2주차는 코드 주석 토글이라 번거로움) | **C** |
| 코드 구조 | **`pipelines/` 분리 + jobs/schedules/sensors 분리** | 자산 17개 + 스케줄/센서. group_name으로 UI 계보 그룹화 | 전반 |

---

## 2. 도메인 & 자산 계보 (무대 장치)

### 2-1. 파이프라인 매핑 (구조 동일, 도메인만 은행으로)

| 파이프라인 | 명세서(이커머스) | 본 설계(은행) | 주기 | 우선순위 | 슬롯 |
|---|---|---|---|---|---|
| P1 | 주문 수집 | **계좌 거래 수집** | 매시간 정각 | 높음(2) | 1 |
| P2 | 매출 집계 | **일별 거래 집계** | P1 후 매시간 | 중간(1) | 1 |
| P3 | 정산 배치 | **지급결제 정산** | 매일 자정 + 긴급 | 최고(3) | 1 |
| P4 | 추천 모델 학습 | **FDS(이상거래탐지) 모델 학습** | 매주 월 02:00 | 낮음(0) | 2 |

### 2-2. 자산 그래프 (lineage)

```
P1 계좌 거래 수집  transactions_raw → transactions_normalized → transactions_dw      (group p1)
                                                                    │
                         ┌──────────────────────────────────────────┤  (공통 상류)
                         ▼                                           ▼
P2 일별 거래 집계   txn_loaded → daily_summary → summary_dw → summary_validated       (group p2)
P3 지급결제 정산    settlement_base → fees_calculated → settlement_finalized          (group p3)
                                                          → settlement_dw → settlement_file
P4 FDS 모델 학습   behavior_logs → fds_features → [fds_model = graph_asset, 2슬롯]     (group p4)
                                                  → fds_model_validated → fds_model_deployed
```

- **`transactions_dw`(P1 T3)가 P2·P3 공통 상류** → 시나리오 C가 ExternalTaskSensor 없이 **lineage로 자연 표현**. 자산 중심 설계의 핵심 이점.
- P4는 독립 계보.

### 2-3. 태스크별 스텁 매핑 (명세서 §4-2 duration/fail)

각 자산은 `shared/stubs.py`의 `spark_task_stub(name, duration_sec, fail)`을 호출.

| 파이프라인 | 자산(태스크) | duration | fail |
|---|---|---|---|
| P1 | transactions_raw / normalized / **dw** | 10 / 5 / 8 | raw·**dw** 설정 가능 (dw = 시나리오 C 대상) |
| P2 | txn_loaded / daily_summary / summary_dw / summary_validated | 5 / 15 / 8 / 5 | validated 설정 가능 |
| P3 | settlement_base / fees_calculated / finalized / dw / file | 8 / 20 / 10 / 8 / 5 | — |
| P4 | behavior_logs / fds_features / **fds_model** / validated / deployed | 15 / 30 / **60(2슬롯)** / 15 / 5 | — |

---

## 3. 구동 레이어 (트리거 · 우선순위 · 스케줄)

| 파이프라인 | 3주차 주력 트리거 | `dagster/priority` | 성격 |
|---|---|---|---|
| P1 거래 수집 | **ScheduleDefinition** 매시간 `0 * * * *` | 2 | 소스(상류 없음) → 스케줄 구동 |
| P2 거래 집계 | **Declarative Automation eager** (`transactions_dw` 갱신 시) | 1 | 순수 파생 → 자동 |
| P3 지급결제 정산 | **AssetSensor**(`transactions_dw` 감시, 1일 1회 가드) + **SLA 데드라인 탈출구** + 수동 긴급 | 3 (최고) | 운영 트리거 혼합(명령형) |
| P4 FDS | **ScheduleDefinition** 매주 월 02:00 `0 2 * * 1` | 0 (최하) | 독립 계보 |

- **우선순위 작동**: run 태그 `dagster/priority`를 **QueuedRunCoordinator**가 읽어 **run 큐 시작 순서**를 정렬(Phase 1).
- **P3 긴급 트리거**: 별도 장치 불필요 — P3 자산 수동 materialize 시 run이 priority=3을 달고 큐 최우선 진입.
- **우선순위 vs 의존성은 직교 축**: 우선순위=자원 경합("누가 먼저 슬롯 가지냐"), 의존성=실행 가능 여부("입력 데이터 있냐"). "P3는 최고 우선순위지만 입력 데이터는 필요하다"는 모순 아님.
- **2×2 비교(6~8주차)**: P2·P3에 반대 메커니즘(P2-sensor, P3-eager) 추가 후 토글 비교 (8주차 비교 문서 소재).

---

## 4. 슬롯 / 동시성 모델 (전부 공식 메커니즘) — 두 층위 구분이 핵심

| Dagster 메커니즘 (범위) | 본 과제 역할 | 우선순위 정렬? |
|---|---|---|
| **Concurrency pool** (cross-run, op 단위, *슬롯*) | `pool="spark"`, limit=3 = 명세서 3슬롯. 동시 op 수 제한 | **❌ 안 함** (가용성/도착순) |
| **Run queue** (`QueuedRunCoordinator`, run 단위) | run 태그 `dagster/priority`로 시작 순서 정렬 | **✅ 함** (run granularity) |
| Executor `max_concurrent` (단일 run 내부) | P4 graph_asset 내부 병렬 op 2개가 한 run에서 병렬로 뜨도록 multiprocess 기본값 | (run 내부) |
| `tag_concurrency_limits` (단일 run 내, 태그별) | per-run 한정 → cross-run 시나리오 부적합, 미사용 | — |

- **슬롯 가중치**: 슬롯은 op 단위로만 점유(가중치 없음). granularity=run이면 모든 run이 1슬롯 → "P4=2슬롯" 불가 → **granularity=op 의도적 선택**. P4 T3 `fds_model`=`@graph_asset`(내부 `pool="spark"` op 2개) → 슬롯 2개 실점유, UI 계보엔 1노드.
- **핵심 비대칭(2주차 §1-6 확인)**: **우선순위 정렬은 run 큐에서만, 슬롯 풀에선 안 일어난다.** 이 한 줄이 아래 모든 시나리오의 Phase 1 한계를 규정한다.
- 슬롯 누수 방지: `run_monitoring.free_slots_after_run_end_seconds`(기존 `dagster.yaml` 유지).

---

## 5. 시나리오 재현 설계 및 검증 ★ (이 문서의 핵심)

**검증 방식 공통**: 스케줄/센서는 설계상 주기를 *선언*하되, 로컬에서 24시간/일주일을 실제로 돌리지 않는다. **수동 트리거로 시점을 압축해 재현**한다.

### 시나리오 A — 우선순위 선점

- **합격 기준(명세서)**: 우선순위 높은 P3가 낮은 P4보다 슬롯을 먼저 확보하는가? (긴급 정산이 모델 학습에 밀리지 않는가)
- **세팅**: P4 materialize 시작 → `fds_model`(graph_asset) 단계 도달 → spark 풀 **2/3 점유, 여분 1**.
- **트리거**: (운영팀 긴급 정산 시뮬레이션) P3 자산 수동 materialize → run priority=3.
- **Phase 1 기대 관측**:
  1. P3 run이 priority=3으로 즉시 시작 admit.
  2. P3 첫 op(`settlement_base`)이 **여분 슬롯 1개를 즉시 claim** → P4 학습과 동시 실행.
  3. 풀 만석(P4=2, P3=1) → 이후 op는 대기.
- **합격(Phase 1)**: P3가 **P4 완료를 기다리지 않고 여분 슬롯으로 즉시 시작**. = "여분 슬롯이 긴급성을 보장"(명세서 슬롯 설계 의도 실증).
- **Phase 1 한계 → Phase 2**: 풀 만석에서 슬롯이 빌 때 대기 중인 P3 op과 P4 op 중 **P3 op을 우선 선택**(op 단위 끼어들기)하는 것은 풀이 우선순위를 안 써서 **불가**. → Phase 2(Celery): op 큐가 `dagster-celery/priority` 합산 정렬 → 빈 슬롯을 P3 op이 우선 확보. 충실 재현.
- **두 Phase 공통**: **선점(실행 중 op 중단)은 없음**. "선점"의 실체 = 여분 슬롯 + 슬롯-free 시 우선 선택. (8주차 didimdp 시사점)

### 시나리오 B — 동시성 제어

- **합격 기준(명세서)**: 총 요청 슬롯(5) > 한계(3)일 때 대기열이 **우선순위 순서로 소비**되는가?
- **세팅/트리거**: 월요일 자정 시뮬레이션 — P1·P3·P4 동시 수동 트리거. P2는 P1 완료 후 eager로 합류. 우선순위 P3(3)>P1(2)>P2(1)>P4(0). 슬롯 피크: 1+1+1+2 = **5 > 3**.
- **Phase 1 기대 관측 (두 층위로 나눠서 정직하게)**:
  - **Layer 2 spark 풀(limit 3)**: 슬롯 한계를 강제 — 동시 op ≤ 3. P4 `fds_model`(2슬롯)은 풀 여유 ≥2일 때만 진입. **그러나 풀은 우선순위를 안 쓴다.**
  - **Layer 1 run 큐**: **우선순위 정렬은 여기서만.** 관측하려면 run 동시성(`max_concurrent_runs`)을 조여 run이 실제로 *큐잉*되게 해야 함 → 그래야 "대기 run이 우선순위 순으로 시작"이 보인다.
- **합격(Phase 1)**: (a) spark 풀이 슬롯 한계 3을 준수, (b) **run 큐가 우선순위 순서로 소비**(run granularity) — 최하위 P4가 가장 뒤로 밀림.
- **Phase 1 한계 → Phase 2**: "슬롯 반납 시 우선순위 순 소비"를 **슬롯(op) granularity로 충실히** 보는 것은 Phase 1 불가(풀이 우선순위 미적용). → Phase 2(Celery)에서 공유 op 큐가 우선순위 정렬 → 슬롯 단위 충실 재현.
- **8주차 서사**: Dagster OSS는 "슬롯 한계"(풀)와 "우선순위 정렬"(run 큐)을 **다른 층위로 분리**해, 둘 중 어느 하나만으로는 *우선순위 정렬된 가중 슬롯 소비*가 안 된다. Celery가 둘을 통합한다. → didimdp 개선 제안의 직접 사례.

### 시나리오 C — 의존성 + 선택적 재실행 (+ SLA 탈출구 확장)

- **합격 기준(명세서)**: P1 T3 실패로 P2·P3 차단 → `transactions_dw`만 재실행(T1·T2 제외) → P2·P3 자동 unblock.
- **세팅**: P1 실행, 단 `transactions_dw`만 `fail=True`(run config 토글) → raw·normalized 성공, dw 실패.
- **관측 1 (차단)**: dw 실패 → P2(eager) 상류 미충족으로 미발화, P3(sensor) 미발화. → **P2·P3 차단 확인**.
- **조치 (선택 재실행)**: `transactions_dw` 자산만 re-materialize(fail 해제). raw/normalized **재실행 안 함**.
- **관측 2 (unblock)**: dw 성공 → eager 조건 충족 → P2 자동 materialize. AssetSensor가 dw 이벤트 감지 → P3 자동 트리거. → **둘 다 자동 재개 확인**.
- **합격(Phase 1, 전부 재현 가능)**: (1) `transactions_dw`만 재실행(상류 미실행), (2) P2·P3 자동 unblock. *우선순위/슬롯과 무관해 Phase 1에서 완전 재현된다.*
- **C-확장 (SLA 데드라인 탈출구)**: `transactions_dw` 미복구 + 컷오프(05:00) 경과 시뮬레이션 → **P3 강제 실행 + 경보**로 06:00 SLA 보장. 평소 경로(컷오프 전 dw 재실행 → 정상 데이터로 P3)는 C를 그대로 보존. (구현 4~7주차)

---

## 6. 코드 구조

```
dagster/
  definitions.py                       # 진입점: 모든 assets/jobs/schedules/sensors/automation 등록
  pipelines/
    p1_transactions/assets.py          # transactions_raw → _normalized → _dw      (group p1)
    p2_aggregation/assets.py           # txn_loaded → daily_summary → summary_dw → summary_validated (group p2, eager)
    p3_settlement/assets.py            # settlement_base → ... → settlement_file    (group p3)
    p4_fds/assets.py                   # behavior_logs → fds_features → fds_model(graph_asset) → ... (group p4)
  jobs.py                              # 파이프라인별 asset job (run 우선순위 태그 부착 지점)
  schedules.py                         # P1 매시간, P4 매주 월 02:00
  sensors.py                           # P3 AssetSensor + SLA 데드라인 탈출구
  experiments/                         # 2주차 실험 보존 (priority_demo_job, cross_job_priority)
  .dagster_home/dagster.yaml           # spark pool limit=3, granularity=op, QueuedRunCoordinator, run_monitoring
  requirements.txt
```

- `define_asset_job(..., tags={"dagster/priority": "<n>"})`로 우선순위 부착. 스케줄·센서·수동 트리거가 이 job을 타겟. (P2 eager run의 우선순위 태그 부착 방식은 구현 시 확인.)
- 파이프라인 = 디렉토리 + `group_name` → UI 계보 그룹화.
- 기존 `p1_order_collection/` → `pipelines/p1_transactions/` (orders_* → transactions_*).
- 2주차 실험(`priority_demo_job.py`, `cross_job_priority.py`)은 `experiments/`로 이동·보존(삭제 안 함, 8주차 비교 참조).
- `shared/stubs.py`의 `spark_task_stub` 그대로 재사용.

---

## 7. 3주차 산출물 매핑 (명세서 §5)

| 명세서 3주차 항목 | 본 설계 대응 |
|---|---|
| 4개 파이프라인 DAG 설계(태스크 수준) | §2 자산 계보 + §2-3 스텁 매핑 |
| 환경 셋업 + 스텁 + Spark 연동 확인 | 기존 venv 복원 완료 / `shared/stubs.py` 재사용 / `dagster.yaml` 확장 |
| 기본 파이프라인 골격 구현 | §6 코드 구조 (P2·P3·P4 추가, P1 은행 도메인 전환) |
| A/B/C 시나리오 사전 검증 | §5 시나리오 재현 설계 및 검증 ★ |
