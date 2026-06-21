import sys
from pathlib import Path

# 이 파일(shared/tests/)의 상위 디렉토리(shared/)를 파이썬 경로에 추가
# → "from stubs import ..." 가 동작하도록
sys.path.insert(0, str(Path(__file__).parents[1]))

import pytest
from stubs import spark_task_stub


def test_stub_completes():
    # 정상 실행 시 예외 없이 완료되는지 검증
    # duration_sec=1 로 짧게 설정해 테스트 속도 확보
    spark_task_stub("test_task", duration_sec=1, fail=False)


def test_stub_raises_on_fail():
    # fail=True 시 지정된 메시지로 예외가 발생하는지 검증
    # pytest.raises: 블록 안에서 해당 예외가 나지 않으면 테스트 실패
    with pytest.raises(Exception, match="test_task intentionally failed"):
        spark_task_stub("test_task", duration_sec=0, fail=True)