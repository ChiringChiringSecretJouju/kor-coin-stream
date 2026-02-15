# connection/utils structure

This package is organized by purpose:

- `logging/`
  - `log_phases.py`: phase constants for structured logs
  - `logging_mixin.py`: scope-aware logging helper mixin
  - `pydantic_filter.py`: BaseModel filter (`exclude_none=True`)

- `subscriptions/`
  - `subscription.py`: subscription merge/extract helpers
  - `subscription_ack.py`: ACK decision helpers

- `symbols/`
  - `symbol_utils.py`: symbol/base/quote extraction and parsing

- `market_data/`
  - `dispatch/`: common parser dispatcher base
  - `parsers/`: parser base abstractions
  - `tickers/`: exchange ticker parsers + dispatchers
  - `trades/`: exchange trade parsers + dispatchers
  - `common/`: parse/dict/time/format helpers used by market-data parsing
