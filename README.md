# 📊 Dataroma Investment Analysis Suite

**Comprehensive institutional investment analysis powered by real portfolio data from 81 top managers**

This repository provides a complete solution for analyzing institutional investment data from [Dataroma](https://www.dataroma.com), tracking **$1.7+ trillion** in assets under management across **3,311 current holdings** and **57,575 historical activities**.

## 🔥 **Live Analysis Dashboard**

### 🎯 Current Market Opportunities (Last 3 Quarters)
Transform complex institutional data into actionable investment opportunities:

| **Analysis** | **Description** | **Visualization** |
|-------------|-----------------|-------------------|
| **🚀 Momentum Analysis** | High-momentum stocks with value opportunities | [📊 Chart](analysis/current/visuals/momentum_analysis_current.png) |
| **📈 New Positions** | Fresh institutional acquisitions with conviction levels | [📊 Chart](analysis/current/visuals/new_positions_current.png) |
| **💰 52-Week Extremes** | Value buying at lows vs profit-taking at highs | [📊 Chart](analysis/current/visuals/52_week_analysis_current.png) |
| **🔍 Low-Price Accumulation** | Institutional bets on sub-$50 stocks | [📊 Chart](analysis/current/visuals/low_price_accumulation_current.png) |
| **⚡ Price Opportunities** | Multi-tier value plays across price ranges | [📊 Chart](analysis/current/visuals/price_opportunities_current.png) |
| **🔄 Portfolio Changes** | Major allocation shifts and concentration moves | [📊 Chart](analysis/current/visuals/portfolio_changes_current.png) |

---

## 📊 **Current Analysis Reports**

### 🎯 **Core Investment Opportunities**
| Report | Description | Link |
|--------|-------------|------|
| **Momentum Stocks** | Recent buying activity trends with scores | [📄 CSV](analysis/current/momentum_stocks.csv) |
| **New Positions** | Fresh manager acquisitions | [📄 CSV](analysis/current/new_positions.csv) |
| **Deep Value Plays** | Undervalued institutional picks | [📄 CSV](analysis/current/deep_value_plays.csv) |
| **High Conviction Low Price** | Best value + conviction combination | [📄 CSV](analysis/current/high_conviction_low_price.csv) |
| **Under Radar Picks** | Hidden gems with manager interest | [📄 CSV](analysis/current/under_radar_picks.csv) |
| **Contrarian Opportunities** | Against-the-trend institutional plays | [📄 CSV](analysis/current/contrarian_opportunities.csv) |

### 📈 **52-Week Extremes Trading**
| Report | Description | Link |
|--------|-------------|------|
| **52-Week Low Buys** | Value hunting near yearly lows | [📄 CSV](analysis/current/52_week_low_buys.csv) |
| **52-Week High Sells** | Profit-taking near yearly highs | [📄 CSV](analysis/current/52_week_high_sells.csv) |

### 💰 **Price-Based Value Screening**
| Report | Description | Link |
|--------|-------------|------|
| **Stocks Under $5** | Ultra-low price institutional bets | [📄 CSV](analysis/current/stocks_under_$5.csv) |
| **Stocks Under $10** | Sub-$10 manager favorites | [📄 CSV](analysis/current/stocks_under_$10.csv) |
| **Stocks Under $20** | Affordable growth opportunities | [📄 CSV](analysis/current/stocks_under_$20.csv) |
| **Stocks Under $50** | Mid-price institutional picks | [📄 CSV](analysis/current/stocks_under_$50.csv) |
| **Stocks Under $100** | Sub-$100 value plays | [📄 CSV](analysis/current/stocks_under_$100.csv) |
| **Value Price Opportunities** | Multi-tier price analysis | [📄 CSV](analysis/current/value_price_opportunities.csv) |

### 🏢 **Portfolio Activity Analysis**
| Report | Description | Link |
|--------|-------------|------|
| **Concentration Changes** | Major portfolio allocation shifts | [📄 CSV](analysis/current/concentration_changes.csv) |
| **Highest Portfolio Concentration** | Most focused institutional positions | [📄 CSV](analysis/current/highest_portfolio_concentration.csv) |
| **Most Sold Stocks** | Recent institutional exit activity | [📄 CSV](analysis/current/most_sold_stocks.csv) |

---

## 📈 **Historical Analysis (18-Year Perspective)**

### Long-Term Market Intelligence
Transform decades of institutional data into strategic insights:

| **Historical Analysis** | **Description** | **Visualization** |
|------------------------|-----------------|-------------------|
| **📅 Quarterly Activity Timeline** | 18 years of institutional trading patterns | [📊 Chart](analysis/historical/visuals/quarterly_activity_timeline.png) |
| **🏛️ Crisis Response Analysis** | How managers navigated major market crises | [📊 Chart](analysis/historical/visuals/crisis_response_comparison.png) |
| **💎 Multi-Decade Conviction** | Stocks held consistently across decades | [📊 Chart](analysis/historical/visuals/multi_decade_conviction.png) |
| **🔄 Stock Life Cycles** | Complete institutional holding patterns | [📊 Chart](analysis/historical/visuals/stock_life_cycles.png) |

### 📅 **Historical Data Reports**
| Report | Description | Link |
|--------|-------------|------|
| **Quarterly Activity Timeline** | 73 quarters of market activity trends | [📄 CSV](analysis/historical/quarterly_activity_timeline.csv) |
| **Crisis Response Analysis** | Manager behavior during market downturns | [📄 CSV](analysis/historical/crisis_response_analysis.csv) |
| **Multi-Decade Conviction** | Long-term institutional favorites | [📄 CSV](analysis/historical/multi_decade_conviction.csv) |
| **Stock Life Cycles** | Complete holding pattern analysis | [📄 CSV](analysis/historical/stock_life_cycles.csv) |

---

## 🚀 **Quick Start**

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt
```

### Run Complete Analysis
```bash
# Run scraper for fresh data 
python3 datorama_scrape.py

# Generate fresh analysis (uses cached data if available)
python3 analyze_holdings.py
```
---

## 🤖 **Technical Features**

### Smart Analysis Engine
- **✅ Real-Time Processing**: Current vs historical data separation
- **✅ Enhanced Visualizations**: Multi-dimensional charts with actionable tables
- **✅ Advanced Scoring**: Multi-factor ranking algorithms for opportunity identification
- **✅ Manager Intelligence**: Performance tracking with name resolution
- **✅ Activity Classification**: Sophisticated buy/sell/add/reduce pattern analysis

### Key Algorithms
- **🎯 Momentum Detection**: Recent institutional accumulation patterns
- **💰 Value Identification**: Price-based opportunity screening across multiple thresholds
- **📊 Concentration Analysis**: Portfolio allocation shifts and risk metrics
- **🏆 Performance Scoring**: Multi-dimensional stock and manager evaluation
- **📈 Trend Analysis**: Historical pattern recognition and forecasting

---

## 🔬 **Research Applications**

Transform raw institutional data into investment intelligence:

### 🎯 **Stock Discovery**
- **High Conviction**: Which stocks have the strongest institutional backing?
- **Hidden Gems**: Under-the-radar picks with smart money accumulation
- **Momentum Plays**: Recent institutional buying with price momentum
- **Value Opportunities**: Stocks being bought near 52-week lows

### 📊 **Market Intelligence**
- **Sector Rotation**: Which industries are seeing increased institutional interest?
- **Price Action**: How do institutional flows correlate with stock performance?
- **Risk Assessment**: Concentration levels and portfolio diversification trends
- **Timing Signals**: When do successful managers typically buy and sell?

### 👥 **Manager Research**
- **Performance Leaders**: Which managers have the most consistent track records?
- **Stock Selection**: Who are the best individual stock pickers?
- **Portfolio Strategy**: How do different managers approach position sizing?
- **Market Timing**: Which managers excel at buying lows and selling highs?

---

## 📋 **Analysis Workflow**

1. **Data Collection**: Scraping of 81 manager portfolios from Dataroma
2. **Data Processing**: Clean, normalize, and enrich with historical performance data
3. **Opportunity Identification**: Multi-factor scoring across momentum, value, and conviction
4. **Visualization Generation**: Create actionable charts with embedded opportunity tables  
5. **Report Generation**: Export comprehensive CSV reports for further analysis
6. **Automated Updates**: Refresh analysis with new quarterly filings

---

## ⚠️ **Important Disclaimer**

This tool is designed for **educational and research purposes only**. 

- **Not Investment Advice**: All analysis is for informational purposes
- **Due Diligence Required**: Always conduct your own research before investing
- **Risk Awareness**: Past performance does not guarantee future results
- **Data Limitations**: Analysis based on publicly available 13F filings with inherent delays

---

## 📄 **Data Source & Attribution**

All institutional holdings data sourced from [Dataroma.com](https://www.dataroma.com) - the premier resource for tracking superinvestor portfolios and institutional activities.

**Special Thanks**: Warren Buffett's Berkshire Hathaway, Bill Ackman's Pershing Square, and other legendary investors whose transparency enables this research.

---

## 🚀 **Next Steps**

1. **Explore Current Analysis**: Start with `analysis/README.md` for latest opportunities
2. **Check Visualizations**: Review the 6 current opportunity charts for actionable insights  
3. **Dive Into Data**: Use CSV reports for detailed stock-by-stock analysis
4. **Historical Context**: Review `analysis/historical/` for long-term performance trends
5. **Custom Analysis**: Fork the repo and customize for your research needs