"""
통합 직렬화기 - JSON과 Avro 바이트 데이터를 자동으로 구분 처리
"""

from __future__ import annotations

from typing import Any, Callable
import orjson


class UnifiedValueSerializer:
    """
    통합 Value 직렬화기
    
    - bytes 데이터: 그대로 통과 (이미 직렬화됨)
    - 기타 데이터: JSON 직렬화
    """
    
    def __init__(self, json_serializer: Callable[[Any], bytes] | None = None):
        self.json_serializer = json_serializer or self._default_json_serializer
    
    def __call__(self, value: Any) -> bytes | None:
        """직렬화 수행"""
        if value is None:
            return None
            
        if isinstance(value, bytes):
            # 이미 직렬화된 바이트 데이터 (Avro 등)
            return value
        else:
            # JSON 직렬화
            return self.json_serializer(value)
    
    @staticmethod
    def _default_json_serializer(value: Any) -> bytes:
        """기본 JSON 직렬화기 (orjson 사용)"""
        return orjson.dumps(value)


class UnifiedKeySerializer:
    """
    통합 Key 직렬화기
    
    - bytes 데이터: 그대로 통과
    - str 데이터: UTF-8 인코딩
    - 기타 데이터: JSON 직렬화
    """
    
    def __init__(self, json_serializer: Callable[[Any], bytes] | None = None):
        self.json_serializer = json_serializer or self._default_json_serializer
    
    def __call__(self, key: Any) -> bytes | None:
        """직렬화 수행"""
        if key is None:
            return None
            
        if isinstance(key, bytes):
            # 이미 직렬화된 바이트 키
            return key
        elif isinstance(key, str):
            # 문자열 키 → UTF-8 인코딩
            return key.encode('utf-8')
        else:
            # JSON 직렬화
            return self.json_serializer(key)
    
    @staticmethod
    def _default_json_serializer(key: Any) -> bytes:
        """기본 JSON 직렬화기 (orjson 사용)"""
        return orjson.dumps(key)


def create_unified_serializers() -> tuple[UnifiedValueSerializer, UnifiedKeySerializer]:
    """통합 직렬화기 생성"""
    return UnifiedValueSerializer(), UnifiedKeySerializer()
