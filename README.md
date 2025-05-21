# AIGC (AI-Generated Content Platform)

This project provides a full-stack pipeline for analyzing and evaluating AI-generated content across thousands of bots using real-time behavioral metrics.

## 🚀 Highlights

- Real-time pipelines powered by Kafka + Flink.
- Data warehouse: StarRocks with modeling support.
- Bot-level analytics: CTR, Chat Start Rate, Depth.
- A/B testing with Bayesian uplift via GrowthBook.
- Visualization via Metabase dashboards.

## 🗂 Project Layout

```
AIGC/
├── bayes/                # Bayesian uplift modeling scripts
├── bots/                 # Bot-level metric evaluation
├── chat/                 # Chat depth and behavior metrics
├── click/                # Click rate calculations
├── experiments/          # A/B experiment result processing
├── retention/            # Retention metrics and analysis
├── subscribe/            # Subscription and payment metrics
├── test/                 # Testing scripts and validations
├── utils/                # Common utilities and shared logic
├── zip/                  # Output compression utilities
└── run_all_metrics.py    # Main execution entry point
```

## 📦 Getting Started

```bash
git clone https://github.com/Islene888/AIGC.git
cd AIGC/AIGC

# Optional: create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run main pipeline
python run_all_metrics.py
```

## 📊 Metrics Tracked

- CTR / Chat Start Rate
- Chat Depth per User
- Retention (D1, D3, D7)
- Subscription & LTV
- Bayesian uplift & win rate

## 📄 License

MIT License © 2025 Mengyuan (Ella) Zhao
