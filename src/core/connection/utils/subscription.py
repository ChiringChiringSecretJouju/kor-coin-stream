from __future__ import annotations

from typing import Any


def extract_subscription_symbols(params: dict[str, Any] | list[Any]) -> list[str]:
    """구독 요청 파라미터에서 심볼 목록을 추출합니다.
    
    이 함수는 SubscriptionManager를 대체하기 위해 순수 함수로 추출되었습니다.
    BaseWebsocketHandler에서 ACK 발행 등을 위해 사용됩니다.

    지원 키: symbols, codes, pair, pairs, symbol, product_ids, args, instId
    지원 구조: Flat Dict, List of Dicts, Nested Dict (params key)
    
    Args:
        params: 거래소에 보낼 구독 요청 Payload (JSON 변환 전 객체)
        
    Returns:
        추출된 심볼 문자열 리스트 (중복 제거됨)
    """

    def collect_from_dict(d: dict[str, Any], bag: list[str]) -> None:
        target_keys = (
            "symbols", "codes", "pair", "pairs", 
            "symbol", "product_ids", "args", "instId"
        )
        for key in target_keys:
            v = d.get(key)
            if v is None:
                continue
                
            if isinstance(v, list):
                for x in v:
                    if isinstance(x, (str, int)):
                        s = str(x)
                        # 중복 접두어 방지 (KRW-KRW-BTC -> KRW-BTC)
                        if s.startswith("KRW-KRW-"):
                            s = s[4:]
                        bag.append(s)
            elif isinstance(v, (str, int)):
                s = str(v)
                if s.startswith("KRW-KRW-"):
                    s = s[4:]
                bag.append(s)
        
        # 중첩 params 처리
        p = d.get("params")
        if isinstance(p, dict):
            for key in target_keys:
                v = p.get(key)
                if isinstance(v, list):
                    for x in v:
                        if isinstance(x, (str, int)):
                            s = str(x)
                            if s.startswith("KRW-KRW-"):
                                s = s[4:]
                            bag.append(s)

    collected: list[str] = []
    
    if isinstance(params, dict):
        collect_from_dict(params, collected)
    elif isinstance(params, list):
        for item in params:
            if isinstance(item, dict):
                collect_from_dict(item, collected)
            elif isinstance(item, str):
                # 리스트 자체가 심볼 목록인 경우 (드물지만 방어적으로 처리)
                collected.append(item)

    # 중복 제거 및 입력 순서 보존 (Python 3.7+ dict는 순서 보존됨)
    # set만 쓰면 순서가 뒤섞일 수 있으므로 dict.fromkeys 사용 권장
    # 여기서는 간단히 loop로 처리
    seen: set[str] = set()
    result: list[str] = []
    for s in collected:
        if s and s not in seen: # 빈 문자열 제외
            seen.add(s)
            result.append(s)
            
    return result


def merge_subscription_params(
    current: dict[str, Any] | list[Any], 
    new_symbols: list[str],
) -> dict[str, Any] | list[Any]:
    """
    기존 파라미터와 신규 심볼을 병합하여 새로운 파라미터 생성.
    
    Zero-Downtime Dynamic Update를 위해 사용됩니다.
    
    전략:
    - Upbit 등 대부분의 거래소가 '전체 심볼 리스트'를 다시 보내야 하는(Overwrite) 구조이므로,
      Incremental(추가분만 전송) 방식 대신 Merged(전체 누적) 방식을 기본으로 합니다.
    - 입력 dict를 얕은 복사(shallow copy)하여 원본 오염을 방지합니다.
    """
    if not current:
        return current
        
    # 2. List 타입인 경우 (Upbit, Bithumb 등 배열 요청 방식 대응)
    if isinstance(current, list):
        updated_list = [item.copy() if isinstance(item, dict) else item for item in current]
        
        # 'codes' 또는 'symbols' 키를 가진 객체 찾아서 업데이트
        for item in updated_list:
            if not isinstance(item, dict):
                continue
                
            for key in ("codes", "symbols", "symbol", "product_ids"):
                if key in item:
                    existing_val = item[key]
                    existing_set = set()
                    
                    if isinstance(existing_val, list):
                        existing_set.update(str(x) for x in existing_val)
                    elif isinstance(existing_val, str):
                        existing_set.add(existing_val)
                        
                    existing_set.update(new_symbols)
                    item[key] = list(existing_set)
                    return updated_list # 첫 번째 매칭되는 객체만 처리하고 반환
        
        return updated_list

    # 3. Dict 타입인 경우
    if not isinstance(current, dict):
        return current
        
    updated_params = current.copy()
    
    # 심볼 키 탐색 (우선순위: symbols -> codes -> symbol -> code)
    # 대부분 symbols(Binance, OKX) 또는 codes(Upbit, Bithumb)임
    target_key = None
    for key in ("symbols", "codes", "symbol", "code", "pair", "pairs", "product_ids", "args"):
        if key in updated_params:
            target_key = key
            break
    
    # 키를 못 찾았거나 중첩 구조 등 복잡한 경우는 병합 포기 (안전성 우선)
    if not target_key:
        return updated_params
        
    # 기존 심볼 가져오기
    existing_val = updated_params[target_key]
    existing_set = set()
    
    if isinstance(existing_val, list):
        existing_set.update(str(x) for x in existing_val)
    elif isinstance(existing_val, str):
        existing_set.add(existing_val)
        
    # 신규 심볼 병합
    existing_set.update(new_symbols)
    
    # 결과 반영 (리스트로 변환)
    updated_params[target_key] = list(existing_set)
    
    return updated_params
