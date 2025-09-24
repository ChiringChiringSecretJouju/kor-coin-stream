#!/usr/bin/env python3
"""
누락된 토픽들을 즉시 생성하는 스크립트
"""

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
import sys


def create_topics():
    """누락된 토픽들을 생성합니다."""

    # Kafka 설정
    conf = {"bootstrap.servers": "kafka:19092,kafka2:29092,kafka3:39092"}
    admin_client = AdminClient(conf)

    # 생성할 토픽들 (이름, 파티션, 복제본)
    topics_config = [
        # 실시간 데이터 토픽들 (지역별)
        ("ticker-data.korea", 6, 1),
        ("ticker-data.na", 12, 1),
        ("ticker-data.eu", 6, 1),
        ("ticker-data.asia", 12, 1),
        ("orderbook-data.korea", 6, 1),
        ("orderbook-data.na", 12, 1),
        ("orderbook-data.eu", 6, 1),
        ("orderbook-data.asia", 12, 1),
        ("trade-data.korea", 6, 1),
        ("trade-data.na", 12, 1),
        ("trade-data.eu", 6, 1),
        ("trade-data.asia", 12, 1),
        # Consumer가 소비하는 토픽들
        ("ws.status.korea", 1, 1),
        ("ws.status.na", 1, 1),
        ("ws.status.eu", 1, 1),
        ("ws.status.asia", 1, 1),
        ("ws.disconnection.korea", 1, 1),
        ("ws.disconnection.na", 1, 1),
        ("ws.disconnection.eu", 1, 1),
        ("ws.disconnection.asia", 1, 1),
        # 기본 토픽들
        ("ws.command", 3, 1),
        ("ws.error", 3, 1),
        ("ws.dlq", 3, 1),
        # 메트릭 토픽들
        ("ws.counting.message.korea", 3, 1),
        ("ws.counting.message.na", 3, 1),
        ("ws.counting.message.eu", 3, 1),
        ("ws.counting.message.asia", 3, 1),
        # 연결 성공 이벤트 토픽들
        ("ws.connect_success.korea", 1, 1),
        ("ws.connect_success.na", 1, 1),
        ("ws.connect_success.eu", 1, 1),
        ("ws.connect_success.asia", 1, 1),
    ]

    # NewTopic 객체 생성
    new_topics = []
    for topic_name, partitions, replication in topics_config:
        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=partitions,
                replication_factor=replication,
            )
        )

    print(f"📋 생성할 토픽 목록 (총 {len(new_topics)}개):")
    for topic_name, partitions, replication in topics_config:
        print(f"  - {topic_name} (파티션: {partitions}, 복제본: {replication})")

    try:
        print(f"\n🔨 토픽 생성 중...")

        # 토픽 생성 요청
        fs = admin_client.create_topics(new_topics, operation_timeout=30)

        # 결과 확인
        success_count = 0
        for topic, f in fs.items():
            try:
                f.result()  # 결과 대기
                print(f"✅ {topic} 생성 성공")
                success_count += 1
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"ℹ️  {topic} 이미 존재함")
                    success_count += 1
                else:
                    print(f"❌ {topic} 생성 실패: {e}")

        print(f"\n🎉 토픽 생성 완료! ({success_count}/{len(new_topics)}개 성공)")
        return True

    except Exception as e:
        print(f"❌ 토픽 생성 중 오류 발생: {e}")
        return False


if __name__ == "__main__":
    print("🚀 누락된 토픽들 생성 시작")
    print("📡 Kafka 서버: localhost:9092")

    success = create_topics()
    if success:
        print("\n✨ 이제 실시간 데이터 스트리밍이 정상 동작할 것입니다!")
    else:
        print("\n💥 토픽 생성 실패. Kafka 서버 상태를 확인해주세요.")
        sys.exit(1)
