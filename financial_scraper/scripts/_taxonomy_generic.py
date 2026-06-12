#!/usr/bin/env python3
"""Generic, --prefix-driven thematic taxonomy for a paper corpus.

Holds an axes registry per corpus prefix; classifies {prefix}_papers_*_fulltext
.parquet (multi-label, title+abstract) and emits {prefix}_taxonomy.md +
{prefix}_labeled.parquet. Feeds the generic review builder.

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_taxonomy_generic.py --prefix ml_futures
"""

import argparse
import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

AXES = {
    "ml_futures": [
        ("Method", "RNN / LSTM / GRU", r"\blstm\b|\bgru\b|recurrent neural|\brnn\b"),
        ("Method", "CNN", r"\bcnn\b|convolutional"),
        ("Method", "Transformer / attention", r"transformer|attention[- ]based|\bbert\b"),
        ("Method", "Reinforcement learning", r"reinforcement learning|\bdrl\b|deep q|q-learning|actor[- ]critic"),
        ("Method", "Classical ML (SVM/RF/boosting)", r"support vector|\bsvm\b|random forest|gradient boost|xgboost|\bknn\b|decision tree"),
        ("Method", "Hybrid / ensemble / decomposition", r"hybrid|ensemble|\bemd\b|wavelet|decomposition|fusion"),
        ("Method", "Genetic / evolutionary", r"genetic algorithm|evolutionary|genetic programming"),
        ("Prediction target", "Price / return forecasting", r"price (?:prediction|forecast)|return (?:prediction|forecast)|forecasting.{0,15}price"),
        ("Prediction target", "Direction / movement", r"direction|movement|up.{0,3}down|trend prediction"),
        ("Prediction target", "Volatility", r"volatilit"),
        ("Prediction target", "Trading strategy / signals", r"trading (?:strateg|system|signal|rule)|algorithmic trading|backtest"),
        ("Asset / market", "Energy (crude / gas)", r"crude|\boil\b|natural gas|wti|brent|energy futures"),
        ("Asset / market", "Metals (gold / copper)", r"gold|silver|copper|metal|nickel|aluminium"),
        ("Asset / market", "Agriculture", r"agricultur|corn|soybean|wheat|crop|grain|coffee|sugar"),
        ("Asset / market", "Equity index futures", r"index futures|stock index|s&p|csi 300|nifty|equity index"),
        ("Asset / market", "Interest-rate / bond futures", r"interest rate futures|bond futures|treasury futures"),
        ("Asset / market", "FX / crypto futures", r"currency futures|fx futures|bitcoin|crypto"),
        ("Cross-cutting themes", "High-frequency / intraday", r"high[- ]frequency|intraday|tick data|limit order"),
        ("Cross-cutting themes", "Technical indicators", r"technical (?:indicator|analysis)|moving average|\brsi\b|macd"),
        ("Cross-cutting themes", "Sentiment / news", r"sentiment|news|text"),
    ],
    "cta_trend": [
        ("Strategy", "Trend following", r"trend[- ]following|trend strateg|\bcta\b|managed futures"),
        ("Strategy", "Time-series momentum", r"time[- ]series momentum|\btsmom\b"),
        ("Strategy", "Cross-sectional momentum", r"cross[- ]sectional momentum|relative momentum"),
        ("Strategy", "Carry", r"\bcarry\b|carry trade"),
        ("Strategy", "Counter-trend / mean-reversion", r"counter[- ]trend|mean[- ]reversion|reversal"),
        ("Topics", "Crisis alpha / tail hedge", r"crisis alpha|tail (?:risk|hedg)|safe haven|crash|drawdown protection|divergent"),
        ("Topics", "Replication / style analysis", r"replicat|style analysis|clone|factor decomposition|return-based"),
        ("Topics", "Performance / survivorship bias", r"performance|survivorship|survivor bias|backfill|persistence"),
        ("Topics", "Risk management / vol scaling", r"volatility (?:scaling|target)|risk management|position sizing|risk budget"),
        ("Topics", "Fees / capacity / turnover", r"\bfee|capacity|turnover|transaction cost|smart leverage"),
        ("Topics", "Diversification", r"diversif|correlation"),
        ("Asset class", "Commodities", r"commodit|crude|metal|agricultur|energy"),
        ("Asset class", "Currencies / FX", r"currenc|\bfx\b|foreign exchange"),
        ("Asset class", "Rates / bonds", r"\bbond\b|fixed income|interest rate|treasury"),
        ("Asset class", "Equity index", r"equity index|stock index|\bs&p"),
        ("Asset class", "Multi-asset", r"multi[- ]asset|cross[- ]asset|diversified futures"),
        ("Method", "Econometric / regression", r"regression|garch|\bvar\b|cointegrat|predictab"),
        ("Method", "Machine learning", r"machine learning|deep learning|neural network|reinforcement learning"),
        ("Method", "Signal rules (MA / breakout)", r"moving average|breakout|channel|donchian|crossover"),
    ],
    "ga_ml": [
        ("Application", "Stock selection / prediction", r"stock (?:selection|prediction|price|return|market)|equity"),
        ("Application", "Trading strategy / system", r"trading (?:strateg|system|rule|signal)|algorithmic trading"),
        ("Application", "Portfolio optimization", r"portfolio (?:optimi|selection|management)|asset allocation"),
        ("Application", "Credit scoring / risk", r"credit (?:scoring|risk)|loan|default prediction"),
        ("Application", "Fraud detection", r"fraud"),
        ("Application", "Bankruptcy / distress prediction", r"bankrupt|financial distress|insolvenc"),
        ("Application", "Price / index forecasting", r"price (?:forecast|prediction)|index (?:forecast|prediction)|exchange rate (?:forecast|prediction)"),
        ("GA role", "Feature selection", r"feature selection|feature subset|variable selection|attribute selection"),
        ("GA role", "Hyperparameter / parameter optimization", r"hyperparameter|parameter (?:optimi|tuning|selection)|optimize.{0,15}parameter"),
        ("GA role", "Weight / portfolio optimization", r"weight optimi|portfolio optimi|optimal weight|asset allocation"),
        ("GA role", "Trading-rule evolution / GP", r"genetic programming|trading rule|evolve.{0,15}rule|rule (?:discovery|generation)"),
        ("GA role", "Neural architecture / topology", r"architecture|topology|neuroevolution|structure optimi"),
        ("Hybrid method", "GA + neural network", r"genetic algorithm.{0,30}neural|neural.{0,30}genetic|ga[- ]?bp|ga[- ]?ann"),
        ("Hybrid method", "GA + SVM", r"genetic.{0,20}support vector|genetic.{0,20}\bsvm\b|ga[- ]?svm"),
        ("Hybrid method", "GA + fuzzy", r"fuzzy"),
        ("Hybrid method", "GA + LSTM / deep", r"genetic.{0,25}(?:lstm|deep learning|cnn)|(?:lstm|deep learning).{0,25}genetic"),
        ("Market", "Forex / currency", r"forex|exchange rate|currenc|\bfx\b"),
        ("Market", "Cryptocurrency", r"crypto|bitcoin"),
        ("Market", "Commodity / futures", r"commodit|crude|gold|futures"),
        ("Market", "China / emerging", r"\bchina\b|chinese|\bcsi\b|india|emerging"),
    ],
    "inst_ownership": [
        ("Disclosure / data", "13F filings", r"\b13[- ]?f\b|form 13|quarterly holding|institutional (?:holding|portfolio) (?:report|disclos)"),
        ("Disclosure / data", "Mutual / hedge fund holdings", r"mutual fund holding|hedge fund (?:holding|portfolio|position)|fund (?:portfolio )?holding"),
        ("Disclosure / data", "Disclosure timing / lag / front-running", r"filing lag|disclosure (?:timing|lag|delay)|front[- ]run|stale (?:holding|disclos)|reporting (?:delay|lag)"),
        ("Strategy / signal", "Copycat / clone / mimicking", r"copycat|clone (?:portfolio|strateg)|mimicking portfolio|replicat|piggyback"),
        ("Strategy / signal", "Best ideas / conviction", r"best ideas|conviction|concentrated (?:holding|position)|high[- ]conviction"),
        ("Strategy / signal", "Changes in ownership / flows", r"change.{0,15}(?:institutional )?ownership|ownership change|institutional (?:demand|flow|buying|selling)|trade imbalance"),
        ("Strategy / signal", "Smart money / informed trading", r"smart money|informed (?:institution|trading|investor)|skilled (?:institution|manager)|information (?:advantage|asymmetr)"),
        ("Mechanism", "Herding / crowding", r"herding|crowd|feedback trading|correlated trading"),
        ("Mechanism", "Price impact / pressure", r"price (?:impact|pressure)|demand curve|downward[- ]sloping|liquidity provision"),
        ("Mechanism", "Return predictability / alpha", r"return predictab|abnormal return|\balpha\b|outperform|cross[- ]section of (?:stock )?returns|risk[- ]adjusted"),
        ("Mechanism", "Monitoring / governance spillover", r"monitor|governance|engagement|stewardship"),
        ("Investor type", "Hedge funds", r"hedge fund"),
        ("Investor type", "Mutual funds", r"mutual fund"),
        ("Investor type", "Pension / banks / insurers", r"pension|insurance (?:compan|fund)|bank trust|endowment"),
        ("Investor type", "Blockholders / activists", r"blockholder|activis|\b5%|large shareholder"),
        ("Investor type", "Retail vs institutional", r"retail|individual investor|household"),
    ],
    "factor_conditioning": [
        ("Conditioning mechanism", "Cross-sectional interaction / double-sort", r"interaction (?:effect|term)|double[- ]?sort|conditional (?:sort|double)|interact\w* (?:with|between|across)|two[- ]way sort"),
        ("Conditioning mechanism", "Nonlinear / ML interactions", r"nonlinear|non[- ]linear|machine learning|deep learning|neural network|gradient boost|random forest|tree[- ]based|deep factor"),
        ("Conditioning mechanism", "Conditional factor model / IPCA", r"conditional (?:factor|beta|asset pric|model)|instrumented principal component|\bipca\b|time[- ]varying (?:beta|loading|exposure)|characteristic[- ]managed|conditional alpha"),
        ("Conditioning mechanism", "Time-series / regime / macro state", r"factor timing|regime[- ]switch|business cycle|macroeconomic (?:state|condition|variable)|state[- ]dependent|conditioning (?:variable|information)"),
        ("Conditioning variable", "Macroeconomic / business cycle", r"macroeconomic|business cycle|recession|monetary|inflation|term spread|interest rate"),
        ("Conditioning variable", "Volatility / risk state", r"volatilit|risk[- ]on|risk aversion|uncertainty|market state|flight[- ]to"),
        ("Conditioning variable", "Firm characteristics", r"characteristic|firm[- ]level|fundamental|size|book[- ]to[- ]market|profitab|quality|investment"),
        ("Conditioning variable", "Sentiment / attention", r"sentiment|attention|investor mood|disagreement"),
        ("Method / econometrics", "Instrumented / latent factors", r"instrumented principal component|\bipca\b|latent factor|autoencoder|factor[- ]?vae|pca"),
        ("Method / econometrics", "Kernel / nonparametric", r"nonparametric|kernel|semiparametric|local (?:linear|regression)|spline"),
        ("Method / econometrics", "SDF / GMM estimation", r"stochastic discount factor|\bsdf\b|\bgmm\b|generalized method of moments|euler equation"),
        ("Method / econometrics", "Portfolio sorts / characteristic regressions", r"portfolio sort|fama[- ]?macbeth|cross[- ]sectional regression|characteristic regression"),
        ("Application", "Equity cross-section", r"cross[- ]section of (?:stock |expected )?returns|stock return|equity"),
        ("Application", "Factor timing / allocation", r"factor timing|factor allocation|factor rotation|timing strateg"),
        ("Application", "Multi-asset / macro", r"multi[- ]asset|cross[- ]asset|currenc|bond|commodit"),
    ],
    "crowding": [
        ("Crowding theme", "Capacity / anomaly decay", r"capacity (?:constraint|of|decay|limit)|arbitrage capacity|anomaly decay|post[- ]publication|out[- ]of[- ]sample (?:decay|decline)|predictability (?:decay|declin|disappear|attenuat)|destroy.{0,20}predictab"),
        ("Crowding theme", "Crowded trades / unwinds", r"crowded trade|crowding|correlated (?:trading|liquidation|unwind)|unwind|quant (?:crisis|meltdown|quake)|fire[- ]?sale|deleverag|liquidation spiral"),
        ("Crowding theme", "Limits to arbitrage", r"limits? to arbitrage|costly arbitrage|arbitrage (?:risk|cost|friction)|noise trader risk|idiosyncratic risk.{0,20}arbitrage"),
        ("Crowding theme", "Momentum / strategy crashes", r"momentum crash|factor crash|strategy crash|drawdown|tail risk|crash risk"),
        ("Measure of crowding", "Short interest / lending", r"short interest|short[- ]sale|securities lending|equity lending|utilization"),
        ("Measure of crowding", "Ownership breadth / institutional overlap", r"breadth of ownership|ownership breadth|institutional (?:ownership|overlap|holding)|common ownership|13f"),
        ("Measure of crowding", "Flows / hedge-fund positioning", r"fund flow|hedge fund (?:positioning|holding|overlap)|smart money flow|capital flow"),
        ("Measure of crowding", "Valuation spreads / comovement", r"valuation spread|value spread|comovement|co[- ]movement|correlation (?:among|of)|return correlation"),
        ("Mechanism", "Herding", r"herding|herd behavior|feedback trading"),
        ("Mechanism", "Price impact / pressure", r"price (?:impact|pressure)|demand curve|downward[- ]sloping"),
        ("Mechanism", "Arbitrage / hedge-fund capital", r"arbitrageur|hedge fund capital|intermediary|funding (?:constraint|liquidity)|leverage"),
        ("Mechanism", "Publication / data-mining effect", r"publication|data[- ]mining|p[- ]hacking|multiple testing|out[- ]of[- ]sample|replicat"),
        ("Asset scope", "Equity factors / anomalies", r"\banomal|factor (?:return|premi|zoo|investing)|cross[- ]section of (?:stock )?returns|stock return"),
        ("Asset scope", "Multi-asset / macro", r"currenc|carry|bond|commodit|multi[- ]asset|cross[- ]asset"),
    ],
    "short_interest": [
        ("Signal / data", "Short interest ratio / days-to-cover", r"short interest (?:ratio)?|days[- ]to[- ]cover|short ratio|relative short interest|\bsir\b"),
        ("Signal / data", "Shorting flow / daily short volume", r"short(?:ing|[- ]sale) (?:flow|volume)|daily short|short[- ]sale volume|order flow"),
        ("Signal / data", "Securities lending / loan fees", r"securities lending|stock (?:loan|lending)|loan fee|lending fee|rebate rate|utilization|specialness|loan supply|lendable"),
        ("Signal / data", "Failures-to-deliver / naked shorting", r"failures? to deliver|\bftd\b|naked short"),
        ("Theme", "Return predictability / overpricing", r"return predictab|cross[- ]section of (?:stock )?returns|overpric|mispric|abnormal return|negative (?:abnormal )?return|underperform"),
        ("Theme", "Informed trading / information content", r"informed (?:trad|short)|information content|private information|skilled short|short sellers? (?:are|anticipate|predict)"),
        ("Theme", "Price discovery / market quality", r"price discovery|market quality|price efficien|informational efficien|liquidity|bid[- ]ask"),
        ("Theme", "Short-sale constraints / divergence of opinion", r"short[- ]sale constraint|constrain|divergence of opinion|heterogeneous belief|disagreement|miller"),
        ("Theme", "Short squeeze / retail / meme", r"short squeeze|meme stock|gamestop|\bgme\b|retail (?:investor|trad)|reddit|wallstreetbets"),
        ("Theme", "Manipulation / bear raids / earnings management", r"manipulat|bear raid|earnings management|fraud|distort|predatory"),
        ("Event / context", "Earnings announcements", r"earnings announcement|earnings news|post[- ]earnings|earnings surprise"),
        ("Event / context", "Regulation / bans / Reg SHO", r"regulation sho|\breg sho\b|short[- ]sale (?:ban|restriction|price[- ]test)|uptick rule|naked.{0,10}ban|2008 ban|emergency"),
        ("Event / context", "Options / derivatives interaction", r"\boption|put[- ]call|derivativ|warrant|convertible"),
        ("Event / context", "Crisis / systemic", r"financial crisis|2008|systemic|fire[- ]sale|run\b"),
        ("Asset scope", "Equity / single-name", r"stock|\bequit|single[- ]name|firm[- ]level|cross[- ]section"),
        ("Asset scope", "ETF / aggregate / index", r"\betf\b|aggregate short|index|market[- ]wide|market return"),
        ("Asset scope", "International / non-US", r"china|\beurope|\bjapan|\bkorea|emerging|cross[- ]countr|non[- ]us"),
    ],
    "statarb": [
        ("Method family", "Pairs trading (distance/cointegration/copula)", r"pairs?[- ]trading|pair trading|distance method|cointegrat\w* pair|copula"),
        ("Method family", "Cointegration / error-correction", r"cointegrat\w+|error[- ]correction|\becm\b|johansen|engle[- ]granger"),
        ("Method family", "Mean reversion / Ornstein-Uhlenbeck", r"mean[- ]revers\w+|mean[- ]revert\w+|ornstein[- ]uhlenbeck|\bou process\b"),
        ("Method family", "Statistical / PCA factor stat-arb", r"eigenportfolio|principal component|\bpca\b|statistical factor|residual (?:reversal|return)|idiosyncratic momentum"),
        ("Method family", "Machine learning / deep / RL", r"machine learning|deep learning|neural network|reinforcement learning|gradient boost|random forest|\blstm\b"),
        ("Method family", "Optimal control / stopping / execution", r"stochastic control|optimal stopping|optimal (?:trading|execution|liquidation)|hamilton[- ]jacobi|free boundary|threshold (?:strateg|rule)"),
        ("Signal construction", "Sparse / mean-reverting portfolio design", r"sparse|mean[- ]revert\w+ portfolio|portfolio selection|box[- ]tiao|predictability|canonical correlation"),
        ("Signal construction", "Kalman / state-space / dynamic hedge", r"kalman|state[- ]space|dynamic hedge|time[- ]varying (?:hedge|beta)|particle filter"),
        ("Signal construction", "Lead-lag / cross-predictability", r"lead[- ]lag|cross[- ]predict|lagged|granger"),
        ("Signal construction", "Spread / relative-value / market-neutral", r"relative value|market[- ]neutral|long[- ]short|spread (?:trading|portfolio)|convergence"),
        ("Asset / market", "US / global equities", r"\bstock|\bequit|s&p|cross[- ]section|single[- ]name"),
        ("Asset / market", "Futures / index / commodities", r"futures|index arbitrage|commodit|calendar spread|stock index"),
        ("Asset / market", "ETF / index products", r"\betf\b|exchange[- ]traded fund|authorized participant|index fund"),
        ("Asset / market", "Crypto / FX", r"cryptocurrenc|bitcoin|crypto|foreign exchange|\bfx\b|currency"),
        ("Cross-cutting", "High-frequency / microstructure", r"high[- ]frequency|intraday|microstructure|limit order|market making|latency"),
        ("Cross-cutting", "Limits to arbitrage / risk", r"limits? (?:to|of) arbitrage|arbitrage risk|funding|noise trader|divergence risk|capacity"),
        ("Cross-cutting", "Transaction costs / implementation", r"transaction cost|implementation|slippage|liquidity|turnover|profitab"),
    ],
}

TITLES = {
    "ml_futures": "Machine Learning for Futures",
    "cta_trend": "CTA / Managed Futures & Trend Following",
    "ga_ml": "Genetic Algorithms in Financial Machine Learning",
    "inst_ownership": "13F Institutional Ownership Disclosure & Equity Alpha",
    "factor_conditioning": "Conditioning & Interacting Equity Factors",
    "crowding": "Crowding in Equity Factors & Strategies",
    "short_interest": "Short Interest & Short Selling in Equities",
    "statarb": "Statistical Arbitrage Methodologies (Equities & Futures)",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, choices=list(AXES))
    args = ap.parse_args()
    p = args.prefix
    axes = AXES[p]

    src = sorted(glob.glob(str(PAPERS / f"{p}_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in axes:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / f"{p}_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w(f"# {TITLES[p]} — Literature Map\n")
    w(f"*Corpus: {N} curated papers, {int(df['has_fulltext'].sum())} with full text. "
      f"Years {int(df['yr'].min())}–{int(df['yr'].max())}. Multi-label, "
      f"auto-classified from title+abstract.*\n")
    cur = None
    for section, sub, _ in axes:
        if section != cur:
            w(f"\n## {section}\n"); cur = section
        key = f"{section} :: {sub}"
        sel = df[df[key]].sort_values("citations", ascending=False)
        if not len(sel):
            continue
        w(f"### {sub}  ·  {len(sel)} papers ({len(sel)*100//N}%)\n")
        for _, r in sel.head(6).iterrows():
            yy = "" if pd.isna(r["yr"]) else int(r["yr"])
            w(f"- **{str(r['title']).strip()}** ({yy}) — {int(r['citations'] or 0)} cites, {r['source']}")
        w("")
    (PAPERS / f"{p}_taxonomy.md").write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {p}_taxonomy.md ({N} papers)")
    for section, sub, _ in axes:
        print(f"  {sub:46s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
