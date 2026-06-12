"""
no-op setproctitle 셰임(shim) — macOS 전용 워크어라운드.

[배경]
macOS에서 Airflow 웹서버(gunicorn)는 워커 프로세스를 fork() 한 뒤
각 워커가 setproctitle C 확장으로 프로세스 이름을 바꾼다.
그런데 setproctitle의 darwin 구현은 CoreFoundation을 호출하는데,
CoreFoundation은 fork() 이후 안전하지 않아(not fork-safe) SIGSEGV로 죽는다.
→ 웹서버 워커가 무한 SIGSEGV 루프에 빠진다.

[해결]
setproctitle은 "ps 출력상의 프로세스 이름 표시"만 담당하는 순수 cosmetic 기능이라
오케스트레이션 동작에는 전혀 영향이 없다. 따라서 동일한 이름의 no-op 모듈을
sys.path 상에서 실제 C 확장보다 앞에 두어(shadow) CoreFoundation 호출 자체를 제거한다.
Airflow/gunicorn의 import 구문은 모두 그대로 동작한다.

[적용 방법]
.venv/bin/activate 에서 PYTHONPATH 앞에 이 디렉토리를 추가.
"""


def setproctitle(title=None):
    return None


def getproctitle():
    return ""


def setthreadtitle(title=None):
    return None


def getthreadtitle():
    return ""
