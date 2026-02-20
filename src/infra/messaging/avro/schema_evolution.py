"""
Schema Registry 호환성 전략 관리

스키마 진화 시 무중단 배포를 위한 호환성 검사 및 관리 기능을 제공합니다.
Confluent Schema Registry의 공식 호환성 룰을 따릅니다.
"""

from dataclasses import dataclass
from enum import Enum

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from src.common.logger import PipelineLogger
from src.config.settings import kafka_settings

logger = PipelineLogger.get_logger("schema_evolution", "infra")


class CompatibilityLevel(str, Enum):
    """스키마 호환성 레벨 (Confluent 공식)

    기본값: BACKWARD (Confluent 권장)
    - Consumer를 토픽 처음부터 rewind 가능
    - Kafka Streams는 BACKWARD만 지원
    """

    # Non-Transitive (최근 1개 스키마만 호환)
    BACKWARD = "BACKWARD"  # 필드 추가 가능 (default 필수), 삭제 불가
    FORWARD = "FORWARD"  # 필드 삭제 가능 (optional만), 추가 불가
    FULL = "FULL"  # 필드 추가/삭제 가능 (모두 optional + default)

    # Transitive (모든 이전 스키마와 호환)
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"

    # No compatibility
    NONE = "NONE"  # 모든 변경 허용 (위험)


@dataclass(slots=True, frozen=True)
class SchemaEvolutionRule:
    """스키마 진화 규칙"""

    compatibility: CompatibilityLevel
    description: str
    field_add: bool  # 필드 추가 가능 여부
    field_remove: bool  # 필드 삭제 가능 여부
    deployment_order: str  # 배포 순서


# 호환성 레벨별 규칙 (Confluent 공식)
EVOLUTION_RULES: dict[CompatibilityLevel, SchemaEvolutionRule] = {
    CompatibilityLevel.BACKWARD: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.BACKWARD,
        description="Consumer 먼저 배포 (필드 추가만 가능)",
        field_add=True,  # ✅ default 값 필요
        field_remove=False,  # ❌
        deployment_order="Consumer → Producer",
    ),
    CompatibilityLevel.FORWARD: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.FORWARD,
        description="Producer 먼저 배포 (필드 삭제만 가능)",
        field_add=False,  # ❌
        field_remove=True,  # ✅ optional만
        deployment_order="Producer → Consumer",
    ),
    CompatibilityLevel.FULL: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.FULL,
        description="양방향 호환 (추가/삭제 가능, 배포 순서 무관)",
        field_add=True,  # ✅ default 값 필요
        field_remove=True,  # ✅ optional만
        deployment_order="순서 무관",
    ),
    CompatibilityLevel.BACKWARD_TRANSITIVE: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.BACKWARD_TRANSITIVE,
        description="모든 이전 스키마와 BACKWARD 호환",
        field_add=True,
        field_remove=False,
        deployment_order="Consumer → Producer",
    ),
    CompatibilityLevel.FORWARD_TRANSITIVE: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.FORWARD_TRANSITIVE,
        description="모든 이전 스키마와 FORWARD 호환",
        field_add=False,
        field_remove=True,
        deployment_order="Producer → Consumer",
    ),
    CompatibilityLevel.FULL_TRANSITIVE: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.FULL_TRANSITIVE,
        description="모든 이전 스키마와 양방향 호환",
        field_add=True,
        field_remove=True,
        deployment_order="순서 무관",
    ),
    CompatibilityLevel.NONE: SchemaEvolutionRule(
        compatibility=CompatibilityLevel.NONE,
        description="호환성 검사 없음 (주의!)",
        field_add=True,
        field_remove=True,
        deployment_order="동시 배포 필요",
    ),
}


class SchemaEvolutionManager:
    """스키마 진화 전략 관리자

    Example:
        >>> manager = SchemaEvolutionManager()
        >>> manager.set_compatibility("ticker-data-value", CompatibilityLevel.BACKWARD)
        >>> rule = manager.get_rule(CompatibilityLevel.BACKWARD)
        >>> print(rule.field_add)  # True
        >>> print(rule.deployment_order)  # "Consumer → Producer"
    """

    def __init__(self) -> None:
        """Schema Registry 클라이언트 초기화"""
        self.registry = SchemaRegistryClient({"url": kafka_settings.schema_register})

    def set_compatibility(
        self,
        subject: str,
        level: CompatibilityLevel = CompatibilityLevel.BACKWARD,
    ) -> None:
        """Subject의 호환성 레벨 설정

        Args:
            subject: 스키마 Subject (예: "ticker-data-value")
            level: 호환성 레벨 (기본: BACKWARD - Confluent 권장)

        Raises:
            Exception: Schema Registry 연결 실패 시
        """
        try:
            self.registry.set_compatibility(subject_name=subject, level=level.value)
            rule = EVOLUTION_RULES[level]
            logger.info(
                f"스키마 호환성 설정 완료: {rule.description}",
                extra={
                    "subject": subject,
                    "level": level.value,
                    "deployment_order": rule.deployment_order,
                },
            )
        except Exception as e:
            logger.error(
                "스키마 호환성 설정 실패",
                extra={"subject": subject, "level": level.value, "error": str(e)},
            )
            raise

    def get_compatibility(self, subject: str) -> CompatibilityLevel:
        """Subject의 현재 호환성 레벨 조회

        Args:
            subject: 스키마 Subject

        Returns:
            현재 호환성 레벨
        """
        try:
            config = self.registry.get_compatibility(subject_name=subject)
            return CompatibilityLevel(config)
        except Exception as e:
            logger.warning(
                "스키마 호환성 조회 실패 (기본값 BACKWARD 사용)",
                extra={"subject": subject, "error": str(e)},
            )
            return CompatibilityLevel.BACKWARD

    @staticmethod
    def get_rule(level: CompatibilityLevel) -> SchemaEvolutionRule:
        """호환성 레벨의 규칙 조회

        Args:
            level: 호환성 레벨

        Returns:
            진화 규칙 (필드 추가/삭제 가능 여부, 배포 순서 등)
        """
        return EVOLUTION_RULES[level]

    def test_compatibility(self, subject: str, schema_str: str) -> bool:
        """새 스키마의 호환성 테스트

        Args:
            subject: 스키마 Subject
            schema_str: 새 스키마 (JSON 문자열)

        Returns:
            True: 호환 가능, False: 호환 불가

        Example:
            >>> new_schema = '{"type": "record", "fields": [...]}'
            >>> is_compatible = manager.test_compatibility("ticker-data-value", new_schema)
        """
        try:
            # Schema Registry API로 호환성 검증
            schema = Schema(schema_str=schema_str, schema_type="AVRO")
            result = self.registry.test_compatibility(
                subject_name=subject,
                schema=schema,
            )

            if result:
                logger.info(
                    "스키마 호환성 검증 성공",
                    extra={"subject": subject},
                )
            else:
                logger.warning(
                    "스키마 호환성 검증 실패",
                    extra={"subject": subject},
                )

            return result
        except Exception as e:
            logger.error(
                "스키마 호환성 테스트 실패",
                extra={"subject": subject, "error": str(e)},
            )
            return False

    def get_schema_versions(self, subject: str) -> list[int]:
        """Subject의 모든 스키마 버전 조회

        Args:
            subject: 스키마 Subject

        Returns:
            스키마 버전 리스트 (예: [1, 2, 3])
        """
        try:
            versions = self.registry.get_versions(subject_name=subject)
            return versions
        except Exception as e:
            logger.error(
                "스키마 버전 조회 실패",
                extra={"subject": subject, "error": str(e)},
            )
            return []

    def validate_evolution_rules(
        self, level: CompatibilityLevel, old_schema: dict, new_schema: dict
    ) -> tuple[bool, str]:
        """스키마 진화 규칙 검증 (로컬 검증)

        Args:
            level: 호환성 레벨
            old_schema: 기존 스키마 (dict)
            new_schema: 새 스키마 (dict)

        Returns:
            (검증 결과, 메시지)
        """
        rule = EVOLUTION_RULES[level]

        old_fields = {f["name"] for f in old_schema.get("fields", [])}
        new_fields = {f["name"] for f in new_schema.get("fields", [])}

        added_fields = new_fields - old_fields
        removed_fields = old_fields - new_fields

        # 필드 추가 검증
        if added_fields and not rule.field_add:
            return (
                False,
                f"필드 추가 불가 ({level.value}): {added_fields}",
            )

        # 필드 삭제 검증
        if removed_fields and not rule.field_remove:
            return (
                False,
                f"필드 삭제 불가 ({level.value}): {removed_fields}",
            )

        return (True, "스키마 진화 규칙 준수")


# 전역 인스턴스 (싱글톤)
_schema_evolution_manager: SchemaEvolutionManager | None = None


def get_schema_evolution_manager() -> SchemaEvolutionManager:
    """Schema Evolution Manager 싱글톤 인스턴스 반환

    Example:
        >>> manager = get_schema_evolution_manager()
        >>> manager.set_compatibility("ticker-data-value", CompatibilityLevel.BACKWARD)
    """
    global _schema_evolution_manager
    if _schema_evolution_manager is None:
        _schema_evolution_manager = SchemaEvolutionManager()
    return _schema_evolution_manager
