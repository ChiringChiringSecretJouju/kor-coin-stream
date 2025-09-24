#!/usr/bin/env python3
"""
Avro 스키마 등록 스크립트

모든 .avsc 파일을 Schema Registry에 등록합니다.
"""

import asyncio
import sys
from src.infra.messaging.avro.schema_registry import (
    register_all_schemas,
    delete_all_subjects,
    SchemaRegistryClient,
)
from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("schema_registration", "main")


async def main():
    """메인 함수"""
    if len(sys.argv) > 1 and sys.argv[1] == "--delete":
        # 모든 스키마 삭제
        logger.info("🗑️  모든 스키마 삭제 중...")
        await delete_all_subjects()
        return

    logger.info("📋 Avro 스키마 등록 시작")

    try:
        # Schema Registry 연결 테스트
        client = SchemaRegistryClient()
        subjects = await client.list_subjects()
        logger.info(f"현재 등록된 주제들: {subjects}")
        await client.close()

        # 모든 스키마 등록
        results = await register_all_schemas()

        logger.info("📊 등록 결과:")
        for subject, schema_id in results.items():
            logger.info(f"  - {subject}: ID {schema_id}")

        logger.info("🎉 스키마 등록 완료!")

    except Exception as e:
        logger.error(f"❌ 스키마 등록 실패: {e}")
        import traceback

        logger.error(f"상세 오류: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
