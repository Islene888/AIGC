# AIGC (AI-Generated Content Platform)

AIGC is a modular analytics framework for evaluating the quality, engagement, and workflow performance of AI-generated conversational bots. This repository supports metric computation, behavioral comparison, and data visualization across different bot tag categories and workflows.

## 🚀 Core Capabilities

- **Workflow Analysis**: Analyze bot performance (chat depth, activation rate, click-through rate) across workflows.
- **Tag-Based Comparison**: Compare performance of bots grouped by custom-defined tags.
- **Insertion & Backfill**: Insert workflow metadata and identifiers into predefined templates.
- **Custom Comparison**: Evaluate workflow vs tag vs overall behavior using Python scripts and CSV inputs.

## 🛠️ Tech Stack

- Python (pandas, matplotlib)
- CSV-based lightweight inputs/outputs
- Manual execution or DolphinScheduler optional

## 📁 Repository Structure

```
AIGC/
├── data_analsis/
│   ├── tag/
│   │   └── by_tag_3_tables.py              # Metrics comparison grouped by tag
│   └── workflow/
│       ├── analsis_by_workflow.py         # Core workflow-level metrics summary
│       ├── dianjilv.py                     # Click-through rate
│       ├── kailiaolv.py                    # Activation/open rate
│       ├── liaotiaoshendu.py              # Chat depth
│       └── liaotiaoshendu1.py             # Alternate depth method
│
├── workflow_insert/
│   ├── AllInOne.csv
│   ├── insert_code.py                     # Insert workflow info into templates
│   ├── workflow2.csv
│   └── workflow_insert.py                 # Insertion logic execution
│
├── comparation/                          # Custom logic for comparing results
└── all/                                   # Full aggregated results
```

## ▶️ Quick Start

```bash
# Clone repo
cd AIGC/AIGC

# Example 1: Run workflow-level analysis
python data_analsis/workflow/analsis_by_workflow.py

# Example 2: Insert workflow data
python workflow_insert/insert_code.py

# Example 3: Run tag-based comparison
python data_analsis/tag/by_tag_3_tables.py
```

## 📊 Metrics Available

- Workflow click-through rate (CTR)
- Chat depth per workflow
- Activation rate
- Bot performance by tag group
- Workflow metadata insertion & batch update

## 📄 License

License © 2025 Mengyuan (Ella) Zhao
