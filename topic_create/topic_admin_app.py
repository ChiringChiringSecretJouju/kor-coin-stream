#!/usr/bin/env python3
"""
Kafka Topic Administration Application

토픽 생성, 삭제, 조회를 위한 CLI 애플리케이션입니다.
기존 data_admin.py의 함수들을 활용하여 사용자 친화적인 인터페이스를 제공합니다.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from confluent_kafka.admin import AdminClient
from confluent_kafka.error import KafkaError, KafkaException

from config.settings import kafka_settings
from infra.messaging.data_admin import delete_all_topics, new_topic_initialization


def list_existing_topics() -> list[str]:
    """기존 토픽 목록을 조회합니다.

    Returns:
        list[str]: 토픽 이름 목록
    """
    conf = {"bootstrap.servers": kafka_settings.BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf=conf)

    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
        topics = list(cluster_metadata.topics.keys())
        return sorted(topics)
    except Exception as e:
        print(f"❌ 토픽 목록 조회 실패: {e}")
        return []


def show_topic_details(topic_names: list[str] | None = None) -> None:
    """토픽의 상세 정보를 출력합니다.

    Args:
        topic_names: 조회할 토픽 이름들. None이면 모든 토픽 조회
    """
    conf = {"bootstrap.servers": kafka_settings.BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf=conf)

    try:
        cluster_metadata = admin_client.list_topics(timeout=10)

        if topic_names is None:
            topics_to_show = cluster_metadata.topics
        else:
            topics_to_show = {
                name: metadata
                for name, metadata in cluster_metadata.topics.items()
                if name in topic_names
            }

        if not topics_to_show:
            print("📭 조회할 토픽이 없습니다.")
            return

        print(f"\n📋 토픽 상세 정보 (총 {len(topics_to_show)}개)")
        print("=" * 80)

        for topic_name, topic_metadata in sorted(topics_to_show.items()):
            partitions = len(topic_metadata.partitions)
            replicas = (
                len(topic_metadata.partitions[0].replicas) if partitions > 0 else 0
            )

            print(f"🏷️  토픽명: {topic_name}")
            print(f"   📊 파티션: {partitions}개")
            print(f"   🔄 복제본: {replicas}개")

            if topic_metadata.error:
                print(f"   ⚠️  에러: {topic_metadata.error}")

            print("-" * 40)

    except Exception as e:
        print(f"❌ 토픽 정보 조회 실패: {e}")


def create_topics_interactive() -> None:
    """대화형 모드로 토픽을 생성합니다."""
    print("\n🚀 대화형 토픽 생성 모드")
    print("=" * 40)

    topics = []
    partitions = []
    replications = []

    while True:
        print(f"\n토픽 #{len(topics) + 1} 정보 입력:")

        # 토픽명 입력
        topic_name = input("토픽명: ").strip()
        if not topic_name:
            print("❌ 토픽명은 필수입니다.")
            continue

        # 파티션 수 입력
        try:
            partition_count = int(input("파티션 수 (기본값: 3): ") or "3")
            if partition_count < 1:
                print("❌ 파티션 수는 1 이상이어야 합니다.")
                continue
        except ValueError:
            print("❌ 파티션 수는 숫자여야 합니다.")
            continue

        # 복제본 수 입력
        try:
            replication_factor = int(input("복제본 수 (기본값: 1): ") or "1")
            if replication_factor < 1:
                print("❌ 복제본 수는 1 이상이어야 합니다.")
                continue
        except ValueError:
            print("❌ 복제본 수는 숫자여야 합니다.")
            continue

        topics.append(topic_name)
        partitions.append(partition_count)
        replications.append(replication_factor)

        print(
            f"✅ 토픽 '{topic_name}' 추가됨 (파티션: {partition_count}, 복제본: {replication_factor})"
        )

        # 계속 추가할지 확인
        continue_input = (
            input("\n다른 토픽을 추가하시겠습니까? (y/N): ").strip().lower()
        )
        if continue_input not in ["y", "yes"]:
            break

    if not topics:
        print("❌ 생성할 토픽이 없습니다.")
        return

    # 생성 확인
    print(f"\n📋 생성할 토픽 목록:")
    for i, (topic, partition, replication) in enumerate(
        zip(topics, partitions, replications), 1
    ):
        print(f"  {i}. {topic} (파티션: {partition}, 복제본: {replication})")

    confirm = (
        input(f"\n{len(topics)}개 토픽을 생성하시겠습니까? (y/N): ").strip().lower()
    )
    if confirm not in ["y", "yes"]:
        print("❌ 토픽 생성이 취소되었습니다.")
        return

    # 토픽 생성 실행
    print("\n🔨 토픽 생성 중...")
    try:
        new_topic_initialization(topics, partitions, replications)
        print("✅ 모든 토픽이 성공적으로 생성되었습니다!")
    except Exception as e:
        print(f"❌ 토픽 생성 실패: {e}")


def create_topics_from_config(config_file: str) -> None:
    """설정 파일에서 토픽 정보를 읽어 생성합니다.

    Args:
        config_file: JSON 설정 파일 경로
    """
    config_path = Path(config_file)

    if not config_path.exists():
        print(f"❌ 설정 파일을 찾을 수 없습니다: {config_file}")
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        topics = []
        partitions = []
        replications = []

        for topic_config in config.get("topics", []):
            topics.append(topic_config["name"])
            partitions.append(topic_config.get("partitions", 3))
            replications.append(topic_config.get("replication_factor", 1))

        if not topics:
            print("❌ 설정 파일에 토픽 정보가 없습니다.")
            return

        print(f"📋 설정 파일에서 {len(topics)}개 토픽 정보를 읽었습니다:")
        for topic, partition, replication in zip(topics, partitions, replications):
            print(f"  - {topic} (파티션: {partition}, 복제본: {replication})")

        print("\n🔨 토픽 생성 중...")
        new_topic_initialization(topics, partitions, replications)
        print("✅ 모든 토픽이 성공적으로 생성되었습니다!")

    except json.JSONDecodeError as e:
        print(f"❌ JSON 설정 파일 파싱 오류: {e}")
    except KeyError as e:
        print(f"❌ 설정 파일에 필수 키가 없습니다: {e}")
    except Exception as e:
        print(f"❌ 토픽 생성 실패: {e}")


def create_sample_config() -> None:
    """샘플 설정 파일을 생성합니다."""
    sample_config = {
        "topics": [
            {
                "name": "ticker-data-value",
                "partitions": 6,
                "replication_factor": 1,
                "description": "실시간 티커 데이터",
            },
            {
                "name": "orderbook-data-value",
                "partitions": 6,
                "replication_factor": 1,
                "description": "실시간 오더북 데이터",
            },
            {
                "name": "trade-data-value",
                "partitions": 6,
                "replication_factor": 1,
                "description": "실시간 거래 데이터",
            },
            {
                "name": "connect-requests-value",
                "partitions": 3,
                "replication_factor": 1,
                "description": "연결 요청 이벤트",
            },
            {
                "name": "error-events-value",
                "partitions": 3,
                "replication_factor": 1,
                "description": "에러 이벤트",
            },
            {
                "name": "metrics-events-value",
                "partitions": 3,
                "replication_factor": 1,
                "description": "메트릭 이벤트",
            },
            {
                "name": "dlq-events-value",
                "partitions": 3,
                "replication_factor": 1,
                "description": "DLQ 이벤트",
            },
        ]
    }

    config_file = "topic_config.json"

    try:
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(sample_config, f, indent=2, ensure_ascii=False)

        print(f"✅ 샘플 설정 파일이 생성되었습니다: {config_file}")
        print("📝 파일을 수정한 후 --config 옵션으로 사용하세요.")

    except Exception as e:
        print(f"❌ 샘플 설정 파일 생성 실패: {e}")


def main() -> None:
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="Kafka 토픽 관리 애플리케이션",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 대화형 토픽 생성
  python topic_admin_app.py create

  # 설정 파일로 토픽 생성
  python topic_admin_app.py create --config topic_config.json

  # 토픽 목록 조회
  python topic_admin_app.py list

  # 특정 토픽 상세 정보 조회
  python topic_admin_app.py info ticker-data-value orderbook-data-value

  # 모든 토픽 삭제 (주의!)
  python topic_admin_app.py delete-all --confirm

  # 샘플 설정 파일 생성
  python topic_admin_app.py sample-config
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="사용 가능한 명령어")

    # create 명령어
    create_parser = subparsers.add_parser("create", help="토픽 생성")
    create_parser.add_argument(
        "--config", "-c", help="JSON 설정 파일 경로 (지정하지 않으면 대화형 모드)"
    )

    # list 명령어
    subparsers.add_parser("list", help="토픽 목록 조회")

    # info 명령어
    info_parser = subparsers.add_parser("info", help="토픽 상세 정보 조회")
    info_parser.add_argument(
        "topics", nargs="*", help="조회할 토픽명들 (지정하지 않으면 모든 토픽)"
    )

    # delete-all 명령어
    delete_parser = subparsers.add_parser("delete-all", help="모든 토픽 삭제")
    delete_parser.add_argument(
        "--confirm",
        action="store_true",
        help="삭제 확인 (이 옵션 없이는 실행되지 않음)",
    )

    # sample-config 명령어
    subparsers.add_parser("sample-config", help="샘플 설정 파일 생성")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    print(f"🔗 Kafka 브로커: {kafka_settings.BOOTSTRAP_SERVERS}")

    try:
        if args.command == "create":
            if args.config:
                create_topics_from_config(args.config)
            else:
                create_topics_interactive()

        elif args.command == "list":
            topics = list_existing_topics()
            if topics:
                print(f"\n📋 기존 토픽 목록 (총 {len(topics)}개):")
                for i, topic in enumerate(topics, 1):
                    print(f"  {i:2d}. {topic}")
            else:
                print("📭 토픽이 없습니다.")

        elif args.command == "info":
            show_topic_details(args.topics if args.topics else None)

        elif args.command == "delete-all":
            if not args.confirm:
                print("❌ 모든 토픽을 삭제하려면 --confirm 옵션을 사용하세요.")
                print("⚠️  이 작업은 되돌릴 수 없습니다!")
                return

            topics = list_existing_topics()
            if not topics:
                print("📭 삭제할 토픽이 없습니다.")
                return

            print(f"⚠️  {len(topics)}개의 모든 토픽을 삭제합니다:")
            for topic in topics:
                print(f"  - {topic}")

            final_confirm = input(
                "\n정말로 모든 토픽을 삭제하시겠습니까? (DELETE 입력): "
            )
            if final_confirm != "DELETE":
                print("❌ 토픽 삭제가 취소되었습니다.")
                return

            print("\n🗑️  모든 토픽 삭제 중...")
            delete_all_topics()
            print("✅ 모든 토픽이 삭제되었습니다.")

        elif args.command == "sample-config":
            create_sample_config()

    except KeyboardInterrupt:
        print("\n❌ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 예상치 못한 오류가 발생했습니다: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
