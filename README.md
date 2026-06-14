# 오케스트레이션 학습 과제 — Airflow vs Dagster

데이터 플랫폼 오케스트레이션 학습 과제. 동일 파이프라인을 Airflow·Dagster로 구현·비교하고,
우선순위 선점·동시성 제어·의존성 전파·선택적 재실행을 직접 구현한다.

- 과제 명세서: `docs/specs/2026-06-04-orchestration-homework-design.md`
- 학습 노트: `docs/study/` (주차별)
- 작업 기록(설계·계획·환경구성): `docs/record/`

> 작업 진행 메모는 `CLAUDE.md` 및 위 문서 참고. 현재 2주차(미니 파이프라인 비교)까지 완료.

---

## 클론 후 환경 복원 가이드

git에는 코드·문서만 들어있다. 가상환경·DB·런타임 데이터는 `.gitignore`로 제외되므로
다른 PC에서는 아래 순서로 환경을 재구성한다.

### 0. 공통 전제 조건

- **Java 11+** (PySpark 필수)
  ```bash
  java -version   # 없으면 설치
  brew install openjdk@21
  # macOS: JAVA_HOME 설정 (Homebrew는 자동 설정 안 함)
  sudo ln -sfn $(brew --prefix openjdk@21)/libexec/openjdk.jdk \
               /Library/Java/JavaVirtualMachines/openjdk-21.jdk
  echo 'export JAVA_HOME=$(brew --prefix openjdk@21)' >> ~/.zshrc
  ```
- **Python 3.11** (3.9는 macOS LibreSSL 문제로 Airflow 실행 불가)
  ```bash
  brew install python@3.11
  ```

### 1. Airflow 환경

```bash
cd airflow/
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv && source .venv/bin/activate

pip install apache-airflow==2.10.4 pyspark pytest \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

# 환경변수 영속화 (venv 활성화 시 자동 적용)
cat >> .venv/bin/activate <<'EOF'
export AIRFLOW_HOME="$(dirname "$VIRTUAL_ENV")/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(dirname "$VIRTUAL_ENV")/dags"
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=10
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$(dirname "$VIRTUAL_ENV")/macos_shims:$PYTHONPATH"
EOF
source .venv/bin/activate

# 실행 (macOS gunicorn SIGSEGV 회피 위해 PYTHONPATH 셰임 필요 — 위에서 설정됨)
airflow standalone
# → http://localhost:8080 (admin / 콘솔 또는 .airflow/standalone_admin_password.txt)
```

**우선순위 실험용 Pool 등록** (별도 탭, venv 활성화):
```bash
airflow pools set spark_pool 1 "우선순위/동시성 실험용 슬롯"
```

> macOS SIGSEGV 셰임(`airflow/macos_shims/setproctitle.py`)은 git에 포함됨.
> `PYTHONPATH`만 위처럼 설정하면 적용된다. 상세 배경: `docs/record/week2/2026-06-13-환경구성.md`

### 2. Dagster 환경

```bash
cd dagster/
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# DAGSTER_HOME 영속화 (dagster.yaml은 git에 포함됨)
echo 'export DAGSTER_HOME="$(dirname "$VIRTUAL_ENV")/.dagster_home"' >> .venv/bin/activate
source .venv/bin/activate

dagster dev -f definitions.py
# → http://localhost:3000
```

### 3. 테스트

```bash
# 둘 중 아무 venv나 활성화한 상태에서 (저장소 루트)
python -m pytest shared/tests/test_stubs.py -v
```

---

## 구현 현황 (2주차까지)

| 항목 | Airflow | Dagster |
|---|---|---|
| P1 미니 파이프라인 | `airflow/dags/p1_order_collection.py` | `dagster/p1_order_collection/assets.py` |
| 우선순위 실험(단일) | `airflow/dags/priority_demo.py` | `dagster/priority_demo_job.py` |
| 크로스 파이프라인 우선순위 | `airflow/dags/cross_dag_priority.py` | `dagster/cross_job_priority.py` |
| 공유 stub | `shared/stubs.py` (양쪽 공유) | |

비교 분석 및 도구 선택 현황: `docs/study/week2/05-비교분석.md` (현재 도구 선택 보류, 3주차 초반 확정 예정)
