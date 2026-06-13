from dagster import Definitions

from p1_order_collection.assets import orders_raw, orders_normalized, orders_dw

# Definitions: Dagster의 진입점.
# 이 객체가 어떤 asset/job/schedule/sensor를 이 코드 위치(code location)에서
# 제공할지 모은다. dagster dev 가 이 파일을 찾아 로드한다.
defs = Definitions(
    assets=[orders_raw, orders_normalized, orders_dw],
)
