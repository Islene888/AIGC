
# AIGC Multi-Dimensional Chatbot Analytics & Automated Evaluation Platform

This project focuses on multi-model performance analysis and core KPI monitoring for AIGC chatbots. It integrates automated scheduling, tag-based grouping comparisons, and KPI visualization, empowering data-driven iteration and business optimization for AI chatbot products.

## 🌟 Highlights

* **Multi-model & Multi-bot Group Comparison**
  Supports grouping analysis based on chatbot interest tags, enabling flexible comparison across different bots or models.
* **Automated Task Scheduling**
  Integrated with scheduling tools like Airflow, supports fully automated script execution, daily/hourly data refresh, and automated report generation.
* **Comprehensive KPI Tracking**
  Covers click-through rate, chat start rate, chat depth, average turns, and more, supporting unified data collection and analysis across business lines.
* **Visualization & Dashboard Integration**
  Analytics results can be automatically written to BI tools such as Metabase for real-time data visualization.

## 📂 Directory Structure

```
AIGC/
├── data_analsis/      # Analysis & ETL scripts (e.g., click rate, chat start rate, chat depth, etc.)
│   ├── workflow/
│   │   ├── click_rate.py
│   │   ├── chat_start_rate.py
│   │   ├── chat_depth.py
│   │   └── ...
│   └── tag/           # Tag-based grouping analysis
├── comparation/       # Multi-model / multi-business line comparison analysis
│   └── ...
├── utils/             # Utility functions
├── dags/              # Airflow DAGs for scheduling
└── ...
```

## ⚙️ Requirements

* Python 3.8+
* SQLAlchemy / pymysql
* pandas / numpy
* Airflow (optional, for automation)
* Metabase API (optional, for dashboard integration)

Install dependencies:

```bash
pip install -r requirements.txt
```

## 🚀 Quick Start

1. **Configure Database & Environment Variables**
   Set database passwords and sensitive info in a `.env` file to avoid hardcoding.

2. **Run Individual Analysis Scripts**
   For example, compute click rate for a specific day:

   ```bash
   python data_analsis/workflow/click_rate.py --start_date 2025-06-01 --end_date 2025-06-01
   ```

3. **Automate End-to-End Pipeline**
   Use Airflow to schedule and automate all core metric analysis and reporting:

   ```bash
   # Example Airflow DAG: run_all_metrics.py
   airflow dags trigger run_all_metrics
   ```

4. **Tag-based Group Comparison**
   Supports custom tag-based KPI comparison for multiple models or business lines:

   ```python
   from data_analsis.tag import by_tag_3_tables
   by_tag_3_tables.main(tag='InterestA')
   ```

5. **Data Dashboard Integration**
   Analytics results can be automatically written to Metabase for BI visualization, requiring zero manual intervention.

## 📝 Notes

* The tag refers to each bot's interest/functional tag for product-level multi-dimensional grouping and comparison, different from the experiment tag in AB testing.
* All ETL and analysis modules can be scheduled and updated automatically, supporting continuous monitoring and daily reporting.
* Supports extension to new KPIs and business lines, adaptable to enterprise applications.







# AIGC 聊天机器人多维度分析与自动化评估平台

本项目专注于 AIGC 聊天机器人的多模型性能分析与核心指标监控，集成自动化调度、标签分组对比和 KPI 可视化等功能，助力 AI 聊天产品的数据驱动迭代与业务优化。

## 🌟 项目亮点

* **多模型&多机器人分组对比**
  支持基于机器人兴趣标签（tag）进行分组分析，灵活对比不同类型 bot 或模型的核心表现。
* **自动化任务调度**
  集成 Airflow 等调度工具，支持全流程脚本自动化执行、每日/每小时数据刷新与报表生成。
* **KPI 指标全量追踪**
  包含点击率、开聊率、聊天深度、人均轮次等，支持多业务线统一采集和分析。
* **可视化与看板对接**
  分析结果可自动写入 Metabase 等 BI 工具，实现数据实时可视化。

## 📂 主要模块结构

```
AIGC/
├── data_analsis/      # 各类分析与 ETL 脚本（如点击率、开聊率、聊天深度等）
│   ├── workflow/
│   │   ├── click_rate.py
│   │   ├── chat_start_rate.py
│   │   ├── chat_depth.py
│   │   └── ...
│   └── tag/           # 按兴趣标签分组分析
├── comparation/       # 多模型/多业务线对比分析
│   └── ...
├── utils/             # 工具类、通用方法
├── dags/              # Airflow DAGs 调度脚本
└── ...
```

## ⚙️ 环境依赖

* Python 3.8+
* SQLAlchemy / pymysql
* pandas / numpy
* Airflow（可选，自动化调度用）
* Metabase API（可选，数据写入看板）

安装依赖：

```bash
pip install -r requirements.txt
```

## 🚀 快速开始

1. **配置数据库与环境变量**
   在 `.env` 文件中配置数据库密码等敏感信息，避免硬编码泄露。

2. **单独运行分析脚本**
   例如统计某日的点击率：

   ```bash
   python data_analsis/workflow/click_rate.py --start_date 2025-06-01 --end_date 2025-06-01
   ```

3. **自动化全流程调度**
   使用 Airflow 批量调度所有核心指标的分析与写入：

   ```bash
   # Airflow DAG 例子：run_all_metrics.py
   airflow dags trigger run_all_metrics
   ```

4. **按标签分组对比分析**
   支持自定义 tag 进行多模型/多业务线 KPI 对比：

   ```python
   from data_analsis.tag import by_tag_3_tables
   by_tag_3_tables.main(tag='兴趣A')
   ```

5. **数据写入可视化看板**
   分析结果可自动写入 Metabase 实现 BI 可视化，无需人工干预。

## 📝 说明

* tag 是每个 bot 的兴趣/功能标签，便于产品多维度分组和横向对比，不等同于 AB 实验的实验标签。
* 项目所有 ETL 与分析模块均可通过调度系统实现自动化和批量更新，适用于持续监控和日报生成。
* 支持扩展新 KPI 与业务线，灵活适配企业级应用。
