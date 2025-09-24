#!/usr/bin/env python3
"""
실시간 배치 토픽 생성 스크립트
"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.infra.messaging.data_admin import new_topic_initialization

def create_batch_topics():
    """실시간 배치 토픽들을 생성합니다."""
    
    topics_to_create = [
        # 한국 지역 토픽
        {
            "name": "ticker-data.korea",
            "partitions": 6,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",  # 24시간
                "compression.type": "zstd",
                "max.message.bytes": "1048576"  # 1MB
            }
        },
        {
            "name": "orderbook-data.korea", 
            "partitions": 6,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd", 
                "max.message.bytes": "2097152"  # 2MB
            }
        },
        {
            "name": "trade-data.korea",
            "partitions": 6,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "1048576"
            }
        },
        # 아시아 지역 토픽
        {
            "name": "ticker-data.asia",
            "partitions": 12,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "1048576"
            }
        },
        {
            "name": "orderbook-data.asia",
            "partitions": 12,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "2097152"
            }
        },
        {
            "name": "trade-data.asia",
            "partitions": 12,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "1048576"
            }
        },
        # 글로벌 지역 토픽
        {
            "name": "ticker-data.global",
            "partitions": 18,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "1048576"
            }
        },
        {
            "name": "orderbook-data.global",
            "partitions": 18,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "2097152"
            }
        },
        {
            "name": "trade-data.global",
            "partitions": 18,
            "replication_factor": 1,
            "config": {
                "cleanup.policy": "delete",
                "retention.ms": "86400000",
                "compression.type": "zstd",
                "max.message.bytes": "1048576"
            }
        }
    ]
    
    print("🚀 실시간 배치 토픽 생성 시작...")
    
    for topic_config in topics_to_create:
        try:
            print(f"📝 토픽 생성 중: {topic_config['name']}")
            
            # new_topic_initialization 함수 사용
            new_topic_initialization(
                topic_name=topic_config["name"],
                num_partitions=topic_config["partitions"],
                replication_factor=topic_config["replication_factor"],
                topic_configs=topic_config.get("config", {})
            )
            
            print(f"✅ 토픽 생성 완료: {topic_config['name']}")
            
        except Exception as e:
            print(f"❌ 토픽 생성 실패: {topic_config['name']} - {e}")
    
    print("\n🎉 실시간 배치 토픽 생성 완료!")

if __name__ == "__main__":
    create_batch_topics()
