# AIGC (AI-Generated Content Platform)

AIGC is a modular analytics framework for evaluating the quality, engagement, and growth performance of AI-generated conversational bots at scale. This repository powers the internal analytics behind FlowGPT’s bot ecosystem, supporting metric computation, A/B testing, and dashboard reporting across thousands of workflows.

## 🚀 Core Capabilities

- **Engagement Analytics**: Compute CTR, chat start rate, depth, and user-level stickiness for 5K+ bots.
- **Retention & LTV Analysis**: Track Day 1/3/7 retention and subscription-based revenue (ARPU, LTV, renewal rate).
- **A/B Experiment Evaluation**: Bayesian uplift modeling to compare variants using GrowthBook labels.
- **Bot Performance Tagging**: Automatically evaluate and rank bots by engagement tiers.
- **Pipeline Automation**: Batch execution with `run_all_metrics.py` and modular logic per metric.

## 🛠️ Tech Stack

- Python, PySpark, SQL
- StarRocks (OLAP DB), Kafka, Flink, S3
- DolphinScheduler (workflow orchestration)
- Metabase, Superset (visualization)
- GrowthBook (A/B test labels)

## 📁 Repository Structure

```
AIGC/
├── bayes/                # Bayesian uplift & win rate calculation
├── bots/                 # Overall bot-level performance computation
├── chat/                 # Chat depth per bot/user computation
├── click/                # Exposure → Click conversion metrics
├── experiments/          # Experiment label parsing & variation comparison
├── retention/            # Daily and cohort-based retention logic
├── subscribe/            # Subscription events, ARPU, renewal rate
├── test/                 # Unit tests & validation logic
├── utils/                # Shared constants, grouping, and data tools
├── zip/                  # Output packaging and compression
└── run_all_metrics.py    # Main controller script to execute all metrics
```

## ▶️ Quick Start

```bash
git clone https://github.com/Islene888/AIGC.git
cd AIGC/AIGC

# Optional environment setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Execute all metrics
python run_all_metrics.py
```

## 📊 Output Metrics (Selected)

- `bot_ctr`, `chat_start_rate`, `chat_depth_user`
- `user_retention_d1/d3/d7`, `ltv`, `payment_rate`
- `uplift`, `win_rate`, `variation_summary`
- `subscribe_dau_rate`, `arpu_by_tag`, `bot_ranking_by_tier`

## 🧪 Example Use Cases

- Identify underperforming bots with low engagement or negative uplift
- Compare model variants in new bot strategies using Bayesian metrics
- Monitor subscription LTV trends across tag or workflow segments

## 📄 License

MIT License © 2025 Mengyuan (Ella) Zhao
