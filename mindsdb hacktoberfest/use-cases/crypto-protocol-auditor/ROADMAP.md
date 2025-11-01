# Roadmap

Feature roadmap for **Crypto Protocol Auditor**.

---

## 🎯 Overview

This is a MindsDB Hacktoberfest 2025 project combining RAG, real-time data, and AI-powered analysis for crypto protocol intelligence. 

---

## ✅ Completed

- [x] MindsDB + PGVector knowledge base (112+ documents)
- [x] Hybrid search (semantic + keyword)
- [x] Live prices (CoinGecko API, 40+ cryptos)
- [x] Market sentiment analysis (News API + Gemini LLM)
- [x] Protocol comparison (2-5 projects)
- [x] Dark theme UI (Gruppo font, responsive)
- [x] Query performance optimization
- [x] KB evaluation & metrics (MRR 1.0, NDCG 1.0)

---

## 🚀 Next Steps

### Phase 1: Search & UX Improvements
- [ ] Query autocomplete
- [ ] Search history (localStorage)
- [ ] Keyboard shortcuts (Cmd+K)
- [ ] Mobile responsiveness

### Phase 2: Data Enrichment
- [ ] GitHub stats (stars, commits, contributors)
- [ ] Social metrics (Twitter, Discord followers)
- [ ] Protocol governance data
- [ ] Developer activity tracking

### Phase 3: **Binance Integration** ⭐
- [ ] Real-time trading data via Binance API
- [ ] Aggregate trade data (klines)
  - Support multiple intervals (1m, 5m, 1h, 1d, etc)
  - Open/close/high/low prices
  - Trading volume & quote asset volume
  - Number of trades & taker buy volume
- [ ] Time-series forecasting for crypto prices
  - Train models on 10K+ historical intervals
  - Predict future price movements
  - Visualize price predictions
- [ ] Enhanced protocol analytics
  - Correlate Binance data with sentiment
  - Track price movements with KB analysis
  - Compare trading patterns across protocols
- [ ] Query trading data in natural language
  - "Show me BTCUSDT trading data for the last 3 days"
  - "What's the correlation between BTC price and sentiment?"

### Phase 4: On-Chain Analytics (Optional)
- [ ] Network stats (TVL, transactions, active addresses)
- [ ] DeFi metrics (liquidity, volume)
- [ ] Staking/validator data
- [ ] Risk scoring model

---

## 📊 Current Performance

| Metric | Value | Status |
|--------|-------|--------|
| KB Search Time | 250ms | ✅ Excellent |
| Mean Reciprocal Rank | 1.0 | ✅ Perfect |
| Hit@10 | 1.0 | ✅ Perfect |
| NDCG@10 | 1.0 | ✅ Perfect |
| Supported Cryptos | 40+ | ✅ Comprehensive |

---

## 🛠️ Tech Stack

- **Frontend**: Next.js 16, React 19, TypeScript, TailwindCSS, Gruppo font
- **Backend**: Next.js API routes (serverless)
- **AI/Search**: MindsDB + PGVector + Google Gemini
- **Data**: PGVector DB, Neon Postgres, CoinGecko API, News API
- **Deployment**: Docker, GitHub

---

## 🎓 Future Considerations

- Portfolio tracking & alerts
- User authentication
- Mobile app (React Native)
- Public API for developers
- White-label solution
- Community features (reviews, discussions)
