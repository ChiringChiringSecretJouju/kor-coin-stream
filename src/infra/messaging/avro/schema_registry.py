"""
Schema Registry 클라이언트 구현

Confluent Schema Registry와의 통신을 담당하며, 스키마 등록, 조회, 호환성 검사 등을 제공합니다.
"""

from __future__ import annotations

import asyncio
from typing import Any
from dataclasses import dataclass
from enum import Enum

import orjson
import aiohttp
from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("schema_registry", "avro")


class CompatibilityLevel(Enum):
    """스키마 호환성 레벨"""

    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


class SchemaRegistryError(Exception):
    """Schema Registry 관련 기본 예외"""

    pass


class SchemaNotFoundError(SchemaRegistryError):
    """스키마를 찾을 수 없을 때 발생하는 예외"""

    pass


class SchemaCompatibilityError(SchemaRegistryError):
    """스키마 호환성 오류"""

    pass


@dataclass
class Schema:
    """스키마 정보를 담는 데이터 클래스"""

    id: int
    version: int
    schema: str
    subject: str


class SchemaRegistryClient:
    """
    Schema Registry 클라이언트

    Confluent Schema Registry와 비동기 통신을 통해 스키마 관리를 수행합니다.
    """

    def __init__(
        self, 
        base_url: str = "http://localhost:8082",
        auth: tuple[str, str] | None = None,
        timeout: float = 30.0
    ):
        self.base_url = base_url
        self.auth = auth
        self.timeout = timeout
        self._session: aiohttp.ClientSession | None = None
        self._schema_cache: dict[int, Schema] = {}
        self._subject_cache: dict[str, list[Schema]] = {}

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _ensure_session(self) -> None:
        """HTTP 세션을 생성하거나 재사용합니다."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            timeout = aiohttp.ClientTimeout(total=self.timeout)

            auth = None
            if self.auth:
                auth = aiohttp.BasicAuth(self.auth[0], self.auth[1])

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                auth=auth,
                headers={"Content-Type": "application/json"},
            )

    async def close(self) -> None:
        """HTTP 세션을 종료합니다."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self, method: str, path: str, data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Schema Registry API 요청을 수행합니다."""
        await self._ensure_session()

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

        try:
            async with self._session.request(
                method,
                url,
                data=orjson.dumps(data) if data else None,
                headers={"Content-Type": "application/json"} if data else None,
            ) as response:
                response_text = await response.text()
                response_data = orjson.loads(response_text)

                if response.status >= 400:
                    error_msg = response_data.get("message", f"HTTP {response.status}")
                    if response.status == 404:
                        raise SchemaNotFoundError(error_msg)
                    else:
                        raise SchemaRegistryError(f"API Error: {error_msg}")

                return response_data

        except aiohttp.ClientError as e:
            raise SchemaRegistryError(f"HTTP request failed: {e}")

    async def register_schema(
        self, subject: str, schema_str: str, schema_type: str = "AVRO"
    ) -> int:
        """
        스키마를 등록하고 스키마 ID를 반환합니다.

        Args:
            subject: 스키마 주제명
            schema_str: Avro 스키마 JSON 문자열
            schema_type: 스키마 타입 (기본값: AVRO)

        Returns:
            등록된 스키마의 ID
        """
        data = {"schema": schema_str, "schemaType": schema_type}

        response = await self._request("POST", f"subjects/{subject}/versions", data)
        schema_id = response["id"]

        logger.info(f"스키마 등록 완료: subject={subject}, id={schema_id}")
        return schema_id

    async def get_schema_by_id(self, schema_id: int) -> Schema:
        """
        스키마 ID로 스키마를 조회합니다.

        Args:
            schema_id: 스키마 ID

        Returns:
            Schema 객체
        """
        # 캐시 확인
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        response = await self._request("GET", f"schemas/ids/{schema_id}")

        schema = Schema(
            id=schema_id,
            version=-1,  # ID로 조회할 때는 버전 정보가 없음
            schema=response["schema"],
            subject="",  # ID로 조회할 때는 주제 정보가 없음
        )

        # 캐시에 저장
        self._schema_cache[schema_id] = schema

        return schema

    async def get_latest_schema(self, subject: str) -> Schema:
        """
        주제의 최신 스키마를 조회합니다.

        Args:
            subject: 스키마 주제명

        Returns:
            최신 Schema 객체
        """
        response = await self._request("GET", f"subjects/{subject}/versions/latest")

        schema = Schema(
            id=response["id"],
            version=response["version"],
            schema=response["schema"],
            subject=subject,
        )

        # 캐시 업데이트
        self._schema_cache[schema.id] = schema
        if subject not in self._subject_cache:
            self._subject_cache[subject] = []

        # 같은 버전이 있으면 교체, 없으면 추가
        existing_versions = [s.version for s in self._subject_cache[subject]]
        if schema.version in existing_versions:
            self._subject_cache[subject] = [
                s for s in self._subject_cache[subject] if s.version != schema.version
            ]
        self._subject_cache[subject].append(schema)

        return schema

    async def get_schema_versions(self, subject: str) -> list[int]:
        """
        주제의 모든 스키마 버전 목록을 조회합니다.

        Args:
            subject: 스키마 주제명

        Returns:
            버전 번호 리스트
        """
        response = await self._request("GET", f"subjects/{subject}/versions")
        return response

    async def check_compatibility(
        self, subject: str, schema_str: str, version: str = "latest"
    ) -> bool:
        """
        스키마 호환성을 검사합니다.

        Args:
            subject: 스키마 주제명
            schema_str: 검사할 스키마 JSON 문자열
            version: 비교할 버전 (기본값: latest)

        Returns:
            호환 가능하면 True, 아니면 False
        """
        data = {"schema": schema_str}

        try:
            response = await self._request(
                "POST", f"compatibility/subjects/{subject}/versions/{version}", data
            )
            return response.get("is_compatible", False)

        except SchemaRegistryError:
            return False

    async def set_compatibility_level(
        self, subject: str, level: CompatibilityLevel
    ) -> None:
        """
        주제의 호환성 레벨을 설정합니다.

        Args:
            subject: 스키마 주제명
            level: 호환성 레벨
        """
        data = {"compatibility": level.value}
        await self._request("PUT", f"config/{subject}", data)

        logger.info(f"호환성 레벨 설정: subject={subject}, level={level.value}")

    async def get_compatibility_level(self, subject: str) -> CompatibilityLevel:
        """
        주제의 호환성 레벨을 조회합니다.

        Args:
            subject: 스키마 주제명

        Returns:
            현재 호환성 레벨
        """
        response = await self._request("GET", f"config/{subject}")
        level_str = response.get("compatibilityLevel", "BACKWARD")
        return CompatibilityLevel(level_str)

    async def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        """
        주제를 삭제합니다.

        Args:
            subject: 삭제할 스키마 주제명
            permanent: 영구 삭제 여부

        Returns:
            삭제된 버전 번호 리스트
        """
        params = "?permanent=true" if permanent else ""
        response = await self._request("DELETE", f"subjects/{subject}{params}")

        # 캐시에서 제거
        if subject in self._subject_cache:
            del self._subject_cache[subject]

        logger.info(f"주제 삭제 완료: subject={subject}, permanent={permanent}")
        return response

    async def list_subjects(self) -> list[str]:
        """
        등록된 모든 스키마 주제 목록을 조회합니다.

        Returns:
            스키마 주제명 리스트
        """
        response = await self._request("GET", "subjects")
        return response


# 유틸리티 함수들
async def register_schema_from_file(
    schema_file_path: str, 
    subject: str,
    base_url: str = "http://localhost:8082"
) -> int:
    """
    파일에서 스키마를 읽어서 등록합니다.
    
    Args:
        schema_file_path: 스키마 파일 경로 (.avsc)
        subject: 스키마 주제명
        base_url: Schema Registry URL
        
    Returns:
        등록된 스키마 ID
    """
    import os
    
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"스키마 파일을 찾을 수 없습니다: {schema_file_path}")
    
    with open(schema_file_path, 'r', encoding='utf-8') as f:
        schema_content = f.read()
    
    client = SchemaRegistryClient(base_url=base_url)
    try:
        schema_id = await client.register_schema(subject, schema_content)
        logger.info(f"스키마 등록 성공: {schema_file_path} -> {subject} (ID: {schema_id})")
        return schema_id
    finally:
        await client.close()


async def register_all_schemas(
    schemas_dir: str = "src/infra/messaging/schemas",
    base_url: str = "http://localhost:8082"
) -> dict[str, int]:
    """
    스키마 디렉토리의 모든 .avsc 파일을 등록합니다.
    
    Args:
        schemas_dir: 스키마 디렉토리 경로
        base_url: Schema Registry URL
        
    Returns:
        {subject: schema_id} 매핑
    """
    import os
    import glob
    
    schema_mappings = {
        "realtime_ticker.avsc": "ticker-data-value",
        "metrics_event.avsc": "metrics-events-value", 
        "connect_success.avsc": "connect-success-events-value",
        "error_event.avsc": "error-events-value",
        "dlq_event.avsc": "dlq-events-value",
        "orderbook_data.avsc": "orderbook-data-value",
        "trade_data.avsc": "trade-data-value",
        "connect_request.avsc": "ws.command-value"
    }
    
    results = {}
    client = SchemaRegistryClient(base_url=base_url)
    
    try:
        for filename, subject in schema_mappings.items():
            file_path = os.path.join(schemas_dir, filename)
            if os.path.exists(file_path):
                try:
                    schema_id = await register_schema_from_file(file_path, subject, base_url)
                    results[subject] = schema_id
                    logger.info(f"✅ {filename} -> {subject} (ID: {schema_id})")
                except Exception as e:
                    logger.error(f"❌ {filename} 등록 실패: {e}")
            else:
                logger.warning(f"⚠️  스키마 파일 없음: {file_path}")
                
        logger.info(f"🎉 총 {len(results)}개 스키마 등록 완료!")
        return results
        
    finally:
        await client.close()


async def delete_all_subjects(base_url: str = "http://localhost:8082") -> None:
    """
    모든 스키마 주제를 삭제합니다 (개발용).
    
    Args:
        base_url: Schema Registry URL
    """
    client = SchemaRegistryClient(base_url=base_url)
    
    try:
        subjects = await client.list_subjects()
        logger.info(f"삭제할 주제들: {subjects}")
        
        for subject in subjects:
            try:
                await client.delete_subject(subject, permanent=True)
                logger.info(f"✅ 주제 삭제: {subject}")
            except Exception as e:
                logger.error(f"❌ 주제 삭제 실패 {subject}: {e}")
                
        logger.info("🗑️  모든 주제 삭제 완료!")
        
    finally:
        await client.close()
