# 암호화폐 거래소 웹소켓 연결 자동화 Makefile
# 10개 거래소를 지역별로 병렬 실행
#
# 단일 구독 거래소 (Coinone, Huobi):
#   - 여러 심볼 요청 시 자동으로 심볼별 개별 연결 생성
#   - 예: SYMBOLS=BTC,ETH,XRP → 3개의 독립적인 WebSocket 연결
#   - Redis 키: ws:connection:coinone:korea:ticker:BTC (심볼 포함)

.PHONY: help all korea asia na eu clean stop ticker orderbook trade batch-test consumer-start consumer-stop \
	all-ticker all-orderbook all-trade korea-ticker korea-orderbook korea-trade \
	asia-ticker asia-orderbook asia-trade na-ticker na-orderbook na-trade \
	eu-ticker eu-orderbook eu-trade upbit-ticker upbit-orderbook upbit-trade \
	bithumb-ticker bithumb-orderbook bithumb-trade coinone-ticker coinone-orderbook coinone-trade \
	korbit-ticker korbit-orderbook korbit-trade \
	binance-ticker binance-orderbook binance-trade \
	huobi-ticker huobi-orderbook huobi-trade \
	bybit-ticker bybit-orderbook bybit-trade \
	okx-ticker okx-orderbook okx-trade \
	coinbase-ticker coinbase-orderbook coinbase-trade kraken-ticker kraken-orderbook kraken-trade \
	bitfinex-ticker bitfinex-orderbook bitfinex-trade

# 기본 설정
PYTHON := PYTHONPATH=.. python
SYMBOLS := BTC,ETH,XRP
REQUEST_TYPE := ticker
SUBSCRIBE_TYPE := ticker

# 배치 수집 테스트용 설정
TEST_SYMBOLS := BTC
TEST_EXCHANGE := upbit
TEST_REGION := korea

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

all: ## 모든 거래소 + 모든 메시지 타입 연결 (ticker, orderbook, trade)
	@echo "$(GREEN)🚀 모든 거래소 + 모든 메시지 타입 연결 시작...$(NC)"
	@$(MAKE) all-ticker &
	@$(MAKE) all-orderbook &
	@$(MAKE) all-trade &
	@echo "$(YELLOW)모든 거래소 + 모든 메시지 타입 병렬 실행 중... Ctrl+C로 중단$(NC)"
	@wait

# 모든 거래소 + 메시지 타입별 연결
all-ticker: ## 모든 거래소 ticker 연결
	@echo "$(GREEN)📊 모든 거래소 ticker 연결 중...$(NC)"
	@$(MAKE) korea-ticker &
	@$(MAKE) asia-ticker &
	@$(MAKE) na-ticker &
	@$(MAKE) eu-ticker &
	@wait

all-orderbook: ## 모든 거래소 orderbook 연결
	@echo "$(GREEN)📈 모든 거래소 orderbook 연결 중...$(NC)"
	@$(MAKE) korea-orderbook &
	@$(MAKE) asia-orderbook &
	@$(MAKE) na-orderbook &
	@$(MAKE) eu-orderbook &
	@wait

all-trade: ## 모든 거래소 trade 연결
	@echo "$(GREEN)💰 모든 거래소 trade 연결 중...$(NC)"
	@$(MAKE) korea-trade &
	@$(MAKE) asia-trade &
	@$(MAKE) na-trade &
	@$(MAKE) eu-trade &
	@wait

korea: ## 한국 거래소 연결 (4개) - ticker만
	@echo "$(GREEN)🇰🇷 한국 거래소 연결 중...$(NC)"
	@$(MAKE) upbit &
	@$(MAKE) bithumb &
	@$(MAKE) coinone &  # 단일 구독 거래소 - 자동으로 심볼별 연결 분리
	@$(MAKE) korbit &
	@wait

# 지역별 + 메시지 타입별 연결
korea-ticker: ## 한국 거래소 ticker 연결
	@$(MAKE) upbit-ticker &
	@$(MAKE) bithumb-ticker &
	@$(MAKE) coinone-ticker &
	@$(MAKE) korbit-ticker &
	@wait

korea-orderbook: ## 한국 거래소 orderbook 연결
	@$(MAKE) upbit-orderbook &
	@$(MAKE) bithumb-orderbook &
	@$(MAKE) coinone-orderbook &
	@$(MAKE) korbit-orderbook &
	@wait

korea-trade: ## 한국 거래소 trade 연결
	@$(MAKE) upbit-trade &
	@$(MAKE) bithumb-trade &
	@$(MAKE) coinone-trade &
	@$(MAKE) korbit-trade &
	@wait

asia: ## 아시아 거래소 연결 (4개) - ticker만
	@echo "$(GREEN)🌏 아시아 거래소 연결 중...$(NC)"
	@$(MAKE) binance &
	@$(MAKE) huobi &  # 단일 구독 거래소 - 자동으로 심볼별 연결 분리
	@$(MAKE) bybit &
	@$(MAKE) okx &
	@wait

asia-ticker: ## 아시아 거래소 ticker 연결
	@$(MAKE) binance-ticker &
	@$(MAKE) huobi-ticker &
	@$(MAKE) bybit-ticker &
	@$(MAKE) okx-ticker &
	@wait

asia-orderbook: ## 아시아 거래소 orderbook 연결
	@$(MAKE) binance-orderbook &
	@$(MAKE) huobi-orderbook &
	@$(MAKE) bybit-orderbook &
	@$(MAKE) okx-orderbook &
	@wait

asia-trade: ## 아시아 거래소 trade 연결
	@$(MAKE) binance-trade &
	@$(MAKE) huobi-trade &
	@$(MAKE) bybit-trade &
	@$(MAKE) okx-trade &
	@wait

na: ## 북미 거래소 연결 (2개) - ticker만
	@echo "$(GREEN)🇺🇸 북미 거래소 연결 중...$(NC)"
	@$(MAKE) coinbase &
	@$(MAKE) kraken &
	@wait

na-ticker: ## 북미 거래소 ticker 연결
	@$(MAKE) coinbase-ticker &
	@$(MAKE) kraken-ticker &
	@wait

na-orderbook: ## 북미 거래소 orderbook 연결
	@$(MAKE) coinbase-orderbook &
	@$(MAKE) kraken-orderbook &
	@wait

na-trade: ## 북미 거래소 trade 연결
	@$(MAKE) coinbase-trade &
	@$(MAKE) kraken-trade &
	@wait

# eu: ## 유럽 거래소 연결 (1개) - ticker만
# 	@echo "$(GREEN)🇪🇺 유럽 거래소 연결 중...$(NC)"
# 	@$(MAKE) bitfinex

# eu-ticker: ## 유럽 거래소 ticker 연결
# 	@$(MAKE) bitfinex-ticker

# eu-orderbook: ## 유럽 거래소 orderbook 연결
# 	@$(MAKE) bitfinex-orderbook

# eu-trade: ## 유럽 거래소 trade 연결
# 	@$(MAKE) bitfinex-trade

# 한국 거래소들
upbit: ## Upbit 연결 (ticker)
	@echo "$(YELLOW)📈 Upbit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

upbit-ticker: ## Upbit ticker 연결
	@echo "$(YELLOW)📊 Upbit ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type ticker \
		--subscribe-type ticker

upbit-orderbook: ## Upbit orderbook 연결
	@echo "$(YELLOW)📈 Upbit orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type orderbook \
		--subscribe-type orderbook

upbit-trade: ## Upbit trade 연결
	@echo "$(YELLOW)💰 Upbit trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange upbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type trade \
		--subscribe-type trade

bithumb: ## Bithumb 연결 (ticker)
	@echo "$(YELLOW)📈 Bithumb 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bithumb \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

bithumb-ticker: ## Bithumb ticker 연결
	@echo "$(YELLOW)📊 Bithumb ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bithumb \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type ticker \
		--subscribe-type ticker

bithumb-orderbook: ## Bithumb orderbook 연결
	@echo "$(YELLOW)📈 Bithumb orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bithumb \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type orderbook \
		--subscribe-type orderbook

bithumb-trade: ## Bithumb trade 연결
	@echo "$(YELLOW)💰 Bithumb trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bithumb \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type trade \
		--subscribe-type trade

coinone: ## Coinone 연결 (ticker) - 단일 구독: 자동으로 심볼별 연결 분리
	@echo "$(YELLOW)📈 Coinone 연결 중... (단일 구독 거래소)$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinone \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

coinone-ticker: ## Coinone ticker 연결 - 자동으로 심볼별 연결 분리
	@echo "$(YELLOW)📊 Coinone ticker 연결 중... (단일 구독 거래소)$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinone \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type ticker \
		--subscribe-type ticker

coinone-orderbook: ## Coinone orderbook 연결
	@echo "$(YELLOW)📈 Coinone orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinone \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type orderbook \
		--subscribe-type orderbook

coinone-trade: ## Coinone trade 연결
	@echo "$(YELLOW)💰 Coinone trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinone \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type trade \
		--subscribe-type trade

korbit: ## Korbit 연결 (ticker)
	@echo "$(YELLOW)📈 Korbit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange korbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

korbit-ticker: ## Korbit ticker 연결
	@echo "$(YELLOW)📊 Korbit ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange korbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type ticker \
		--subscribe-type ticker

korbit-orderbook: ## Korbit orderbook 연결
	@echo "$(YELLOW)📈 Korbit orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange korbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type orderbook \
		--subscribe-type orderbook

korbit-trade: ## Korbit trade 연결
	@echo "$(YELLOW)💰 Korbit trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange korbit \
		--symbols $(SYMBOLS) \
		--region korea \
		--request-type trade \
		--subscribe-type trade

# gopax: ## Gopax 연결 (ticker) - 제거됨
# 	@echo "$(YELLOW)📈 Gopax 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange gopax \
# 		--symbols $(SYMBOLS) \
# 		--region korea \
# 		--request-type $(REQUEST_TYPE) \
# 		--subscribe-type $(SUBSCRIBE_TYPE)

# gopax-ticker: ## Gopax ticker 연결 - 제거됨
# 	@echo "$(YELLOW)📊 Gopax ticker 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange gopax \
# 		--symbols $(SYMBOLS) \
# 		--region korea \
# 		--request-type ticker \
# 		--subscribe-type ticker

# gopax-orderbook: ## Gopax orderbook 연결 - 제거됨
# 	@echo "$(YELLOW)📈 Gopax orderbook 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange gopax \
# 		--symbols $(SYMBOLS) \
# 		--region korea \
# 		--request-type orderbook \
# 		--subscribe-type orderbook

# gopax-trade: ## Gopax trade 연결 - 제거됨
# 	@echo "$(YELLOW)💰 Gopax trade 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange gopax \
# 		--symbols $(SYMBOLS) \
# 		--region korea \
# 		--request-type trade \
# 		--subscribe-type trade

# 아시아 거래소들
binance: ## Binance 연결 (ticker)
	@echo "$(YELLOW)📈 Binance 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange binance \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

binance-ticker: ## Binance ticker 연결
	@echo "$(YELLOW)📊 Binance ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange binance \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type ticker \
		--subscribe-type ticker

binance-orderbook: ## Binance orderbook 연결
	@echo "$(YELLOW)📈 Binance orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange binance \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type orderbook \
		--subscribe-type orderbook

binance-trade: ## Binance trade 연결
	@echo "$(YELLOW)💰 Binance trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange binance \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type trade \
		--subscribe-type trade

huobi: ## Huobi 연결 (ticker) - 단일 구독: 자동으로 심볼별 연결 분리
	@echo "$(YELLOW)📈 Huobi 연결 중... (단일 구독 거래소)$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange huobi \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

huobi-ticker: ## Huobi ticker 연결 - 자동으로 심볼별 연결 분리
	@echo "$(YELLOW)📊 Huobi ticker 연결 중... (단일 구독 거래소)$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange huobi \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type ticker \
		--subscribe-type ticker

huobi-orderbook: ## Huobi orderbook 연결
	@echo "$(YELLOW)📈 Huobi orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange huobi \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type orderbook \
		--subscribe-type orderbook

huobi-trade: ## Huobi trade 연결
	@echo "$(YELLOW)💰 Huobi trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange huobi \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type trade \
		--subscribe-type trade

# mexc: ## MEXC 연결 (ticker) - 제거됨
# 	@echo "$(YELLOW)📈 MEXC 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange mexc \
# 		--symbols $(SYMBOLS) \
# 		--region asia \
# 		--request-type $(REQUEST_TYPE) \
# 		--subscribe-type $(SUBSCRIBE_TYPE)

# mexc-ticker: ## MEXC ticker 연결 - 제거됨
# 	@echo "$(YELLOW)📊 MEXC ticker 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange mexc \
# 		--symbols $(SYMBOLS) \
# 		--region asia \
# 		--request-type ticker \
# 		--subscribe-type ticker

# mexc-orderbook: ## MEXC orderbook 연결 - 제거됨
# 	@echo "$(YELLOW)📈 MEXC orderbook 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange mexc \
# 		--symbols $(SYMBOLS) \
# 		--region asia \
# 		--request-type orderbook \
# 		--subscribe-type orderbook

# mexc-trade: ## MEXC trade 연결 - 제거됨
# 	@echo "$(YELLOW)💰 MEXC trade 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange mexc \
# 		--symbols $(SYMBOLS) \
# 		--region asia \
# 		--request-type trade \
# 		--subscribe-type trade

bybit: ## Bybit 연결 (ticker)
	@echo "$(YELLOW)📈 Bybit 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bybit \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

bybit-ticker: ## Bybit ticker 연결
	@echo "$(YELLOW)📊 Bybit ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bybit \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type ticker \
		--subscribe-type ticker

bybit-orderbook: ## Bybit orderbook 연결
	@echo "$(YELLOW)📈 Bybit orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bybit \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type orderbook \
		--subscribe-type orderbook

bybit-trade: ## Bybit trade 연결
	@echo "$(YELLOW)💰 Bybit trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange bybit \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type trade \
		--subscribe-type trade

gateio: ## Gate.io 연결 (ticker)
	@echo "$(YELLOW)📈 Gate.io 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange gateio \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

gateio-ticker: ## Gate.io ticker 연결
	@echo "$(YELLOW)📊 Gate.io ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange gateio \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type ticker \
		--subscribe-type ticker

gateio-orderbook: ## Gate.io orderbook 연결
	@echo "$(YELLOW)📈 Gate.io orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange gateio \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type orderbook \
		--subscribe-type orderbook

gateio-trade: ## Gate.io trade 연결
	@echo "$(YELLOW)💰 Gate.io trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange gateio \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type trade \
		--subscribe-type trade

okx: ## OKX 연결 (ticker)
	@echo "$(YELLOW)📈 OKX 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange okx \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

okx-ticker: ## OKX ticker 연결
	@echo "$(YELLOW)📊 OKX ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange okx \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type ticker \
		--subscribe-type ticker

okx-orderbook: ## OKX orderbook 연결
	@echo "$(YELLOW)📈 OKX orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange okx \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type orderbook \
		--subscribe-type orderbook

okx-trade: ## OKX trade 연결
	@echo "$(YELLOW)💰 OKX trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange okx \
		--symbols $(SYMBOLS) \
		--region asia \
		--request-type trade \
		--subscribe-type trade

# 북미 거래소들
coinbase: ## Coinbase 연결 (ticker)
	@echo "$(YELLOW)📈 Coinbase 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinbase \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

coinbase-ticker: ## Coinbase ticker 연결
	@echo "$(YELLOW)📊 Coinbase ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinbase \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type ticker \
		--subscribe-type ticker

coinbase-orderbook: ## Coinbase orderbook 연결
	@echo "$(YELLOW)📈 Coinbase orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinbase \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type orderbook \
		--subscribe-type orderbook

coinbase-trade: ## Coinbase trade 연결
	@echo "$(YELLOW)💰 Coinbase trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange coinbase \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type trade \
		--subscribe-type trade

kraken: ## Kraken 연결 (ticker)
	@echo "$(YELLOW)📈 Kraken 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange kraken \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type $(REQUEST_TYPE) \
		--subscribe-type $(SUBSCRIBE_TYPE)

kraken-ticker: ## Kraken ticker 연결
	@echo "$(YELLOW)📊 Kraken ticker 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange kraken \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type ticker \
		--subscribe-type ticker

kraken-orderbook: ## Kraken orderbook 연결
	@echo "$(YELLOW)📈 Kraken orderbook 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange kraken \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type orderbook \
		--subscribe-type orderbook

kraken-trade: ## Kraken trade 연결
	@echo "$(YELLOW)💰 Kraken trade 연결 중...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange kraken \
		--symbols $(SYMBOLS) \
		--region na \
		--request-type trade \
		--subscribe-type trade

# 유럽 거래소들
# bitfinex: ## Bitfinex 연결 (ticker)
# 	@echo "$(YELLOW)📈 Bitfinex 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange bitfinex \
# 		--symbols $(SYMBOLS) \
# 		--region eu \
# 		--request-type $(REQUEST_TYPE) \
# 		--subscribe-type $(SUBSCRIBE_TYPE)

# bitfinex-ticker: ## Bitfinex ticker 연결
# 	@echo "$(YELLOW)📊 Bitfinex ticker 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange bitfinex \
# 		--symbols $(SYMBOLS) \
# 		--region eu \
# 		--request-type ticker \
# 		--subscribe-type ticker

# bitfinex-orderbook: ## Bitfinex orderbook 연결
# 	@echo "$(YELLOW)📈 Bitfinex orderbook 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange bitfinex \
# 		--symbols $(SYMBOLS) \
# 		--region eu \
# 		--request-type orderbook \
# 		--subscribe-type orderbook

# bitfinex-trade: ## Bitfinex trade 연결
# 	@echo "$(YELLOW)💰 Bitfinex trade 연결 중...$(NC)"
# 	@$(PYTHON) producer_main.py \
# 		--exchange bitfinex \
# 		--symbols $(SYMBOLS) \
# 		--region eu \
# 		--request-type trade \
# 		--subscribe-type trade

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

# 메시지 타입별 테스트 명령어들
ticker: ## 티커 메시지 테스트 (배치 수집 포함)
	@echo "$(GREEN)📊 티커 메시지 배치 수집 테스트...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange $(TEST_EXCHANGE) \
		--symbols $(TEST_SYMBOLS) \
		--region $(TEST_REGION) \
		--request-type ticker \
		--subscribe-type ticker

orderbook: ## 오더북 메시지 테스트 (배치 수집 포함)
	@echo "$(GREEN)📈 오더북 메시지 배치 수집 테스트...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange $(TEST_EXCHANGE) \
		--symbols $(TEST_SYMBOLS) \
		--region $(TEST_REGION) \
		--request-type orderbook \
		--subscribe-type orderbook

trade: ## 체결 메시지 테스트 (배치 수집 포함)
	@echo "$(GREEN)💰 체결 메시지 배치 수집 테스트...$(NC)"
	@$(PYTHON) producer_main.py \
		--exchange $(TEST_EXCHANGE) \
		--symbols $(TEST_SYMBOLS) \
		--region $(TEST_REGION) \
		--request-type trade \
		--subscribe-type trade

# 배치 수집 통합 테스트
batch-test: ## 모든 메시지 타입 배치 수집 테스트 (순차 실행)
	@echo "$(GREEN)🚀 배치 수집 시스템 통합 테스트 시작...$(NC)"
	@echo "$(YELLOW)1/3 티커 메시지 테스트 (30초)...$(NC)"
	@timeout 30s $(MAKE) ticker || true
	@sleep 2
	@echo "$(YELLOW)2/3 오더북 메시지 테스트 (30초)...$(NC)"
	@timeout 30s $(MAKE) orderbook || true
	@sleep 2
	@echo "$(YELLOW)3/3 체결 메시지 테스트 (30초)...$(NC)"
	@timeout 30s $(MAKE) trade || true
	@echo "$(GREEN)✅ 배치 수집 통합 테스트 완료$(NC)"

batch-parallel: ## 모든 메시지 타입 병렬 배치 테스트
	@echo "$(GREEN)⚡ 병렬 배치 수집 테스트 시작...$(NC)"
	@$(MAKE) ticker TEST_EXCHANGE=upbit &
	@$(MAKE) orderbook TEST_EXCHANGE=bithumb &
	@$(MAKE) trade TEST_EXCHANGE=coinone &
	@echo "$(YELLOW)병렬 실행 중... Ctrl+C로 중단$(NC)"
	@wait

# Consumer 관리 명령어들
consumer-start: ## Kafka Consumer 시작 (백그라운드)
	@echo "$(GREEN)🎧 Kafka Consumer 시작...$(NC)"
	@nohup $(PYTHON) main.py > consumer.log 2>&1 &
	@echo "$(YELLOW)Consumer가 백그라운드에서 실행 중입니다. 로그: consumer.log$(NC)"

consumer-stop: ## Kafka Consumer 중단
	@echo "$(RED)🛑 Kafka Consumer 중단...$(NC)"
	@pkill -f "python main.py" || true
	@echo "$(GREEN)✅ Consumer 중단 완료$(NC)"

consumer-logs: ## Consumer 로그 실시간 확인
	@echo "$(GREEN)📋 Consumer 로그 실시간 확인 (Ctrl+C로 중단)...$(NC)"
	@tail -f consumer.log 2>/dev/null || echo "$(RED)consumer.log 파일이 없습니다. consumer-start를 먼저 실행하세요.$(NC)"

# 거래소별 메시지 타입 테스트
test-upbit-all: ## Upbit 모든 메시지 타입 테스트
	@echo "$(GREEN)🇰🇷 Upbit 전체 메시지 타입 테스트...$(NC)"
	@$(MAKE) ticker TEST_EXCHANGE=upbit &
	@$(MAKE) orderbook TEST_EXCHANGE=upbit &
	@$(MAKE) trade TEST_EXCHANGE=upbit &
	@wait

test-binance-all: ## Binance 모든 메시지 타입 테스트
	@echo "$(GREEN)🌏 Binance 전체 메시지 타입 테스트...$(NC)"
	@$(MAKE) ticker TEST_EXCHANGE=binance TEST_REGION=asia &
	@$(MAKE) orderbook TEST_EXCHANGE=binance TEST_REGION=asia &
	@$(MAKE) trade TEST_EXCHANGE=binance TEST_REGION=asia &
	@wait

# 성능 테스트 명령어들
perf-test: ## 성능 테스트 (다중 거래소 + 다중 메시지 타입)
	@echo "$(GREEN)⚡ 성능 테스트 시작 (2분간 실행)...$(NC)"
	@timeout 120s $(MAKE) korea REQUEST_TYPE=ticker &
	@timeout 120s $(MAKE) asia REQUEST_TYPE=orderbook &
	@timeout 120s $(MAKE) na REQUEST_TYPE=trade &
	@echo "$(YELLOW)성능 테스트 실행 중... 2분 후 자동 종료$(NC)"
	@wait
	@echo "$(GREEN)✅ 성능 테스트 완료$(NC)"

# 토픽 생성 명령어들
create-topics: ## Kafka 토픽 생성
	@echo "$(GREEN)📝 Kafka 토픽 생성...$(NC)"
	@cd topic_create && $(PYTHON) create_batch_topics.py
	@echo "$(GREEN)✅ 토픽 생성 완료$(NC)"

# 전체 시스템 테스트
full-test: ## 전체 시스템 테스트 (토픽 생성 + Consumer 시작 + 배치 테스트)
	@echo "$(GREEN)🚀 전체 시스템 테스트 시작...$(NC)"
	@$(MAKE) create-topics
	@$(MAKE) consumer-start
	@sleep 5
	@$(MAKE) batch-test
	@$(MAKE) consumer-stop
	@echo "$(GREEN)✅ 전체 시스템 테스트 완료$(NC)"

# 도움말을 기본 타겟으로 설정
.DEFAULT_GOAL := help
