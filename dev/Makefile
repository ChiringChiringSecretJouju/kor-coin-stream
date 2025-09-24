# 암호화폐 거래소 웹소켓 연결 자동화 Makefile
# 10개 거래소를 지역별로 병렬 실행

.PHONY: help all korea asia na eu clean stop

# 기본 설정
PYTHON := python
SYMBOLS := BTC,ETH,XRP
REQUEST_TYPE := ticker
SUBSCRIBE_TYPE := ticker

# 색상 정의
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## 사용 가능한 명령어 표시
	@echo "$(GREEN)암호화폐 거래소 웹소켓 연결 자동화$(NC)"
	@echo ""
	@echo "$(YELLOW)사용 가능한 명령어:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: ## 모든 거래소 연결 (10개)
	@echo "$(GREEN)🚀 모든 거래소 연결 시작...$(NC)"
	@$(MAKE) korea &
	@$(MAKE) asia &
	@$(MAKE) na &
	@$(MAKE) eu &
	@echo "$(YELLOW)모든 지역 병렬 실행 중... Ctrl+C로 중단$(NC)"
	@wait

korea: ## 한국 거래소 연결 (4개)
	@echo "$(GREEN)🇰🇷 한국 거래소 연결 중...$(NC)"
	@$(MAKE) upbit &
	@$(MAKE) bithumb &
	@$(MAKE) coinone &
	@$(MAKE) korbit &
	@wait

asia: ## 아시아 거래소 연결 (5개)
	@echo "$(GREEN)🌏 아시아 거래소 연결 중...$(NC)"
	@$(MAKE) binance &
	@$(MAKE) huobi &
	@$(MAKE) bybit &
	@$(MAKE) gateio &
	@$(MAKE) okx &
	@wait

na: ## 북미 거래소 연결 (2개)
	@echo "$(GREEN)🇺🇸 북미 거래소 연결 중...$(NC)"
	@$(MAKE) coinbase &
	@$(MAKE) kraken &
	@wait

eu: ## 유럽 거래소 연결 (1개)
	@echo "$(GREEN)🇪🇺 유럽 거래소 연결 중...$(NC)"
	@$(MAKE) bitfinex

# 한국 거래소들
upbit: ## Upbit 연결
	@echo "$(YELLOW)📈 Upbit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

bithumb: ## Bithumb 연결
	@echo "$(YELLOW)📈 Bithumb 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bithumb \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

coinone: ## Coinone 연결
	@echo "$(YELLOW)📈 Coinone 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinone \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

korbit: ## Korbit 연결
	@echo "$(YELLOW)📈 Korbit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange korbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

# 아시아 거래소들
binance: ## Binance 연결
	@echo "$(YELLOW)📈 Binance 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange binance \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

huobi: ## Huobi 연결
	@echo "$(YELLOW)📈 Huobi 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange huobi \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

bybit: ## Bybit 연결
	@echo "$(YELLOW)📈 Bybit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bybit \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

gateio: ## Gate.io 연결
	@echo "$(YELLOW)📈 Gate.io 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange gateio \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

okx: ## OKX 연결
	@echo "$(YELLOW)📈 OKX 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange okx \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

# 북미 거래소들
coinbase: ## Coinbase 연결
	@echo "$(YELLOW)📈 Coinbase 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinbase \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

kraken: ## Kraken 연결
	@echo "$(YELLOW)📈 Kraken 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange kraken \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

# 유럽 거래소들
bitfinex: ## Bitfinex 연결
	@echo "$(YELLOW)📈 Bitfinex 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bitfinex \
		--symbols $(SYMBOLS) \
		--region eu \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

# 유틸리티 명령어들
test: ## 단일 거래소 테스트 (Upbit)
	@echo "$(GREEN)🧪 Upbit 테스트 연결...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols BTC \
		--region korea \
		--request-type ticker \
		--subscribe-type ticker

status: ## Kafka Consumer 상태 확인
	@echo "$(GREEN)📊 Kafka Consumer 상태 확인...$(NC)"
	@$(PYTHON) main.py &
	@sleep 5
	@pkill -f "python main.py" || true

clean: ## 실행 중인 프로세스 정리
	@echo "$(RED)🧹 실행 중인 프로세스 정리...$(NC)"
	@pkill -f "producer_main.py" || true
	@pkill -f "main.py" || true
	@echo "$(GREEN)✅ 정리 완료$(NC)"

stop: clean ## 모든 프로세스 중단 (clean과 동일)

# 설정 변경 가능한 타겟들
symbols-btc: ## BTC만 연결
	@$(MAKE) all SYMBOLS=BTC

symbols-major: ## 주요 코인 연결 (BTC,ETH,XRP)
	@$(MAKE) all SYMBOLS=BTC,ETH,XRP

symbols-all: ## 모든 코인 연결 (BTC,ETH,XRP,ADA,DOT)
	@$(MAKE) all SYMBOLS=BTC,ETH,XRP,ADA,DOT

# 개발용 명령어들
dev-korea: ## 개발용: 한국 거래소만 순차 실행
	@echo "$(GREEN)🔧 개발 모드: 한국 거래소 순차 실행$(NC)"
	@$(MAKE) upbit
	@sleep 2
	@$(MAKE) bithumb
	@sleep 2
	@$(MAKE) coinone
	@sleep 2
	@$(MAKE) korbit

dev-test: ## 개발용: 각 지역별 1개씩만 테스트
	@echo "$(GREEN)🔧 개발 모드: 지역별 테스트$(NC)"
	@$(MAKE) upbit &
	@$(MAKE) binance &
	@$(MAKE) coinbase &
	@$(MAKE) bitfinex &
	@wait

# 도움말을 기본 타겟으로 설정
.DEFAULT_GOAL := help
