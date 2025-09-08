# First Step — 환경 설정 (ZAOC 친화형, uv + Python 3.12.10)

이 문서는 **uv** 패키지 관리자와 **Python 3.12.10**을 사용해, ZAOC 원칙(경계 1회 검증 / 내부 dataclass / 단일 Adapter / serde 일원화)을 바로 적용할 수 있도록 환경을 세팅하는 가이드입니다.

> 전제
> - Python: **3.12.10**
> - 패키지 매니저: **uv** (pip 호환 하위 커맨드 제공)
> - 의존성 목록: `requirements.txt` (이미 첨부됨)
>
> 목표
> - 재현 가능한 가상환경(`.venv`) + 빠른 설치(uv)
> - ZAOC 규율을 자동 enforce 할 **프리커밋/린트/타입체크** 골격
> - 직렬화 단일 진입점(`common/serde.py`) 외 `json.dumps` 사용 차단 규칙의 초안

---

## 0) 디렉토리 합의 (초기 레이아웃)

```
kor-coin-stream/
  prom iting/
    first_step.md  ← (이 파일)
  common/
    serde.py       ← 직렬화 단일 진입점(추가 예정)
  core/
    dto/
      io/          ← Pydantic(BaseModel) — I/O 경계
      internal/    ← dataclass(slots/frozen) — 내부 전용
    adapters/
      io_to_internal.py ← 변환 단 1곳
  .venv/           ← uv 가상환경(자동 생성)
```

> **원칙 요약**: I/O=**Pydantic**, 내부=**dataclass**, 변환=**adapters 한 곳**, 직렬화=`common/serde.py`만.

---

## 1) uv 설치 & 확인

```bash
# macOS/Linux (Homebrew)
brew install uv

# 또는 공식 스크립트
curl -LsSf https://astral.sh/uv/install.sh | sh

# 설치 확인
uv --version
```

> 참고: uv는 pip/venv와 호환되는 **초고속** Python 패키지/환경 관리 도구입니다.

---

## 2) Python 3.12.10 설치 & 고정

```bash
# 프로젝트 루트에서 실행
cd kor-coin-stream

# uv로 Python 런타임 설치 (전역 런타임 캐시에 설치)
uv python install 3.12.10

# 프로젝트 가상환경 생성(폴더 로컬 고정)
uv venv --python 3.12.10 .venv

# 활성화 (macOS/Linux)
source .venv/bin/activate
# Windows (PowerShell)
# .venv\\Scripts\\Activate.ps1

# 버전 확인
python -V  # 3.12.10이어야 함
```

> 팀/CI에서도 동일 버전을 강제하려면 `.python-version`(pyenv) 파일에 `3.12.10`을 적어 두는 것도 좋습니다.

---

## 3) 의존성 설치 (requirements.txt 기준)

`requirements.txt`는 현재 다음 패키지를 포함합니다:

```
requests
pandas
numpy
confluent-kafka
pydantic
websocket
websockets
websocket-client
aiokafka
cctx
kafka-python-ng
kafka-python
mmh3
pyyaml
aiohttp
setuptools
cachetools
```

설치:

```bash
# uv의 pip 호환 서브커맨드 사용
uv pip install -r requirements.txt

# 설치 검증 (예: pydantic, pandas, confluent-kafka)
python -c "import pydantic, pandas; print('ok:', pydantic.__version__)"
```

> 이후 의존성 변경 시에도 동일 커맨드로 갱신합니다. (잠정적으로는 `requirements.txt`를 소스 오브 트루스로 유지)

---

## 4) ZAOC 규율을 강제하는 개발 도구 세트(선택/권장)

아래는 **규칙이 깨지지 않도록** 자동화하는 최소 세트입니다. (추후 `promiting/next_step.md`에서 파일 생성 자동화를 추가 권장)

### 4.1 pre-commit

```bash
uv pip install pre-commit ruff mypy
pre-commit --version
```

프로젝트 루트에 `.pre-commit-config.yaml` 초안(예시):

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
        args: ["--fix"]
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.11.2
    hooks:
      - id: mypy
        additional_dependencies: ["pydantic>=2"]
  - repo: local
    hooks:
      - id: forbid-pydantic-in-internal
        name: forbid-pydantic-in-internal
        entry: bash -c 'if grep -R "from pydantic" core/dto/internal core/ 2>/dev/null; then echo "Pydantic import found in internal"; exit 1; fi'
        language: system
      - id: forbid-json-dumps-outside-serde
        name: forbid-json-dumps-outside-serde
        entry: bash -c 'if git diff --cached --name-only | xargs -I{} grep -n "json.dumps(" {} | grep -v "common/serde.py"; then echo "Use common/serde.py only"; exit 1; fi || true'
        language: system
```

활성화:
```bash
pre-commit install
```

### 4.2 mypy (타입검사) 기본 설정

`mypy.ini` 예시:
```ini
[mypy]
python_version = 3.12
warn_unused_ignores = True
warn_redundant_casts = True
strict_optional = True
check_untyped_defs = True
no_implicit_optional = True

[mypy-pydantic.*]
ignore_missing_imports = True
```

### 4.3 Ruff (빠른 린터)
별도 설정 없이도 동작하지만, 필요 시 `ruff.toml`을 추가하세요.

---

## 5) ZAOC 체크리스트 (환경 관점)

- [ ] **I/O= Pydantic(BaseModel)**: `core/dto/io/*` 에만 BaseModel 존재
- [ ] **내부= dataclass(slots/frozen)**: `core/dto/internal/*` 에서만 사용, Pydantic **금지**
- [ ] **변환= adapters 한 곳**: `core/adapters/io_to_internal.py` 에만 변환 로직 존재
- [ ] **직렬화= common/serde.py**: 다른 레이어에서 `json.dumps` 직접 호출 금지
- [ ] **pre-commit**: 위 금지 규칙이 린트/훅으로 강제됨
- [ ] **Python 고정**: 3.12.10 가상환경(`.venv`) 사용이 기본값

---

## 6) 빠른 스모크 테스트

아래 스크립트는 uv 환경과 의존성 로딩이 정상인지, ZAOC 규칙의 핵심(경계=Pydantic, 내부=dataclass)이 동작하는지 짧게 확인합니다.

```bash
python - <<'PY'
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict

class Target(BaseModel):
    model_config = ConfigDict(extra='forbid')
    exchange: str
    region: str
    request_type: str

@dataclass(slots=True, frozen=True)
class Scope:
    exchange: str
    region: str
    request_type: str

# I/O 검증 (경계)
io = Target(exchange='upbit', region='korea', request_type='ticker')
# 내부로 축소
sc = Scope(io.exchange, io.region, io.request_type)
print('OK ->', sc)
PY
```

정상 출력: `OK -> Scope(...)`

---

## 7) 운영 팁
- CI에서 `pre-commit run --all-files` + `mypy` + `ruff`를 통과해야 병합 가능하도록 게이트 설정
- 팀/서버 모두 `uv python install 3.12.10 && uv venv --python 3.12.10` 표준화
- requirements.txt는 단순 소스. 이후 안정화 단계에서 `pyproject.toml`로 이전해도 무방

---

## 부록) 트러블슈팅
- `uv: command not found` → PATH 재확인, 또는 brew/스크립트 재설치
- `python -V`가 3.12.10이 아님 → 가상환경 활성화(`source .venv/bin/activate`) 누락 여부 확인
- pre-commit 훅이 동작하지 않음 → `pre-commit install` 실행 여부 확인

