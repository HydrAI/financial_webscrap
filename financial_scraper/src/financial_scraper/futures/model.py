"""Data model for futures contract specifications."""

from dataclasses import dataclass, field


@dataclass
class FuturesContract:
    exchange: str = ""              # "LME", "CME", "ICE"
    product_name: str = ""          # "LME Steel Rebar FOB Turkey (Platts)"
    ticker: str = ""                # "SR"
    asset_class: str = ""           # energy | metals | agriculture | softs | livestock | financials | emissions
    contract_size: str = ""         # "10 tonnes"
    quote_currency: str = ""        # "US dollars per tonne"
    tick_size: str = ""             # "$0.01 per tonne"
    trading_months: str = ""        # "Every month out to 15 months"
    settlement_type: str = ""       # "Cash settled" | "Physical"
    trading_hours: str = ""         # free-form
    last_trade_date_rule: str = ""  # "Last business day of the contract month"
    source_url: str = ""            # canonical spec page URL
    extra_specs: dict = field(default_factory=dict)
    error: str = ""
    scraped_at: str = ""            # ISO timestamp
