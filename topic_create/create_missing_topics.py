#!/usr/bin/env python3
"""
누락된 실시간 데이터 토픽들을 생성하는 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config.settings import kafka_settings
from src.infra.messaging.data_admin import new_topic_initialization

def create_realtime_topics():
    """실시간 데이터 처리에 필요한 토픽들을 생성합니다."""
    
    # 실시간 데이터 토픽들 (지역별)
    topics = [
        # 한국 지역 실시간 데이터
        "ticker-data.korea",
        "orderbook-data.korea", 
        "trade-data.korea",
        
        # 아시아 지역 실시간 데이터
        "ticker-data.asia",
        "orderbook-data.asia",
        "trade-data.asia",
        
        # 글로벌 지역 실시간 데이터  
        "ticker-data.global",
        "orderbook-data.global",
        "trade-data.global",
        
        # 메트릭 및 에러 토픽들
        "ws.counting.message.korea",
        "ws.counting.message.asia", 
        "ws.counting.message.global",
        "ws.error",
        "ws.dlq",
    ]
    
    # 파티션 수 (지역별 차등)
    partitions = [
        # 한국 지역 (6개)
        6, 6, 6,
        
        # 아시아 지역 (12개)  
        12, 12, 12,
        
        # 글로벌 지역 (18개)
        18, 18, 18,
        
        # 메트릭/에러 토픽들 (3개)
        3, 3, 3, 3, 3,
    ]
    
    # 복제본 수 (모두 1개 - 로컬 개발환경)
    replications = [1] * len(topics)
    
    print(f"📋 생성할 토픽 목록 (총 {len(topics)}개):")
    for topic, partition, replication in zip(topics, partitions, replications):
        print(f"  - {topic} (파티션: {partition}, 복제본: {replication})")
    
    print(f"\n🔨 토픽 생성 중...")
    try:
        new_topic_initialization(topics, partitions, replications)
        print("✅ 모든 토픽이 성공적으로 생성되었습니다!")
        return True
    except Exception as e:
        print(f"❌ 토픽 생성 실패: {e}")
        return False

if __name__ == "__main__":
    print("🚀 실시간 데이터 토픽 생성 시작")
    print(f"📡 Kafka 서버: {kafka_settings.BOOTSTRAP_SERVERS}")
    
    success = create_realtime_topics()
    if success:
        print("\n🎉 토픽 생성 완료! 이제 실시간 데이터 스트리밍이 가능합니다.")
    else:
        print("\n💥 토픽 생성 실패. Kafka 서버 상태를 확인해주세요.")
        sys.exit(1)
