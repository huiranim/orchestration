from dagster import Definitions

# Definitions: Dagster의 진입점.
# 이 객체가 어떤 asset/job/schedule/sensor를 이 코드 위치(code location)에서
# 제공할지 모은다. dagster dev 가 이 파일을 찾아 로드한다.
# 지금은 골격만 — asset은 다음 단계에서 추가한다.
defs = Definitions(assets=[])
