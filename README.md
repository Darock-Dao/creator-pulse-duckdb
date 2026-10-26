# ⚡ CreatorPulse: Real-Time Social Media Velocity & Traction Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![DuckDB](https://img.shields.io/badge/DuckDB-Fast%20In--Process%20OLAP-FFF000?logo=duckdb)
![dbt](https://img.shields.io/badge/dbt-DuckDB-FF694B?logo=dbt)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8%2B-017CEE?logo=apacheairflow)
![Streamlit](https://img.shields.io/badge/Streamlit-1.50-FF4B4B?logo=streamlit)
![YouTube Data API](https://img.shields.io/badge/YouTube%20Data%20API-v3-FF0000?logo=youtube)

**CreatorPulse** is an automated data pipeline that tracks how fast YouTube videos gain views over time. 

Built with **Python**, **DuckDB**, **dbt**, **Apache Airflow**, and **Streamlit**, it transforms one-time view counts into continuous growth curves and momentum metrics.

---

## 📌 The Problem: YouTube Only Gives You the Total, Not the Growth

When you ask the YouTube API for video statistics, it only returns a single lifetime number (for example, *"this video has 1,500,000 views"*). It does **not** tell you *when* those views arrived. 

For creators, marketers, and brands, total views don't tell the whole story. What actually matters is **momentum**:
* **Launch Velocity:** How fast is a new video picking up views in its first 24 to 48 hours?
* **Momentum Spikes:** Is an older video suddenly surging because the algorithm recommended it, or has its growth slowed down?
* **Audience Engagement:** Are people actively liking and commenting as views climb?

### The Solution
**CreatorPulse** periodically takes "snapshots" of video metrics, stores the history locally in **DuckDB**, calculates the hourly growth speed using **dbt**, automates the runs with **Airflow**, and displays live interactive charts in a **Streamlit** dashboard.

---

## 🏗️ Architecture Overview

```text
               ┌────────────────────────────────────────────────────────┐
               │                     Streamlit GUI                      │
               │  - Add / remove creators from a watchlist              │
               │  - View growth curves and hourly speed spikes          │
               └──────────▲──────────────────────────────────▲──────────┘
                          │ (reads metrics)                  │ (manages creators)
                          │                                  ▼
               ┌──────────┴───────────────┐     ┌───────────────────────┐
               │ dbt Analytics Marts      │     │ watchlist             │
               │ - fct_video_velocity     │     │ (channel_id, handle)  │
               │ - mart_channel_trends    │     └───────────┬───────────┘
               └──────────▲───────────────┘                 │
                          │                                 │ (reads active creators)
               ┌──────────┴───────────────┐                 ▼
               │ DuckDB Analytical Layer  │     ┌───────────────────────┐
               │ - raw_video_snapshots    │◄────┤ Airflow Orchestrator  │
               └──────────────────────────┘     │ - Runs every 6 hours  │
                                                │ - Fetches YouTube data│
                                                │ - Loads to DuckDB     │
                                                │ - Runs dbt models     │
                                                └───────────────────────┘
```

---

## 🛠️ Tech Stack & Key Decisions

| Tool | Role | Why It Was Chosen |
| :--- | :--- | :--- |
| **Python & YouTube API v3** | Ingestion | Uses a smart **2-unit quota strategy** that cuts API usage by 98% compared to standard searches. |
| **DuckDB** | In-Process Analytical Database | Fast, serverless columnar OLAP database that runs embedded locally with zero cloud costs or infrastructure overhead. |
| **dbt (`dbt-duckdb`)** | Data Modeling | Calculates view growth and hourly velocity using SQL window functions (`LAG()`). |
| **Apache Airflow** | Workflow Orchestration | Runs the pipeline automatically on a schedule with built-in retries and error handling. |
| **Streamlit & Altair** | Interactive Dashboard | Clean web interface for tracking creators, filtering videos, and zooming into growth curves. |

---

## 🔬 How It Works

### 1. Ingestion: Saving 98% of YouTube API Quota
* YouTube gives free accounts **10,000 points per day**.
* Using the search endpoint costs **100 points** per request (which runs out quickly).
* CreatorPulse avoids search entirely: it looks up the channel's "Uploads" playlist (**1 point**) and batches up to 50 videos at once (**1 point**).
* **Result:** It captures full video metrics for only **2 points per creator**.
* Each snapshot gets a unique ID made from `video_id + timestamp` to make sure repeated runs never create duplicate rows.

### 2. Storage: Staging and Loading into DuckDB
* **Database File:** `data/creator_pulse.duckdb`
* **Watchlist Table (`watchlist`):** Stores tracked channels (`channel_id`, `channel_handle`, `channel_title`, `is_active`). Channels can be paused or resumed anytime.
* **Snapshots Table (`raw_video_snapshots`):** An append-only table recording `view_count`, `like_count`, `comment_count`, and exact timestamps.
* Staged JSON Lines (`.jsonl`) payloads are loaded directly into DuckDB using fast vectorized `read_json_auto()`.

### 3. Transformation: Calculating Speed with dbt
* **Deduplication:** A staging view (`stg_video_snapshots`) cleans up the data and ensures duplicate timestamps are removed using `QUALIFY ROW_NUMBER() = 1`.
* **Hourly Velocity (`fct_video_velocity`):**
  Using SQL window functions (`LAG()`), the model compares the current snapshot against the previous one:
  $$\text{Hourly Speed (views/hr)} = \frac{\text{Current Views} - \text{Previous Views}}{\text{Hours Elapsed Between Snapshots}}$$
  By measuring elapsed time down to the second (`ROUND(DATEDIFF('second', t1, t2) / 3600.0, 4)`), the formula accurately calculates views per hour even when snapshots are taken only minutes apart.
* **Channel Performance Mart (`mart_channel_performance`):** Aggregates overall channel statistics, average speed, engagement rates, and peak launch spikes.
* **Data Quality Tests:** 7 automated tests verify that IDs are unique, values are not null, and links between tables are valid.

### 4. Orchestration: Automatic Runs with Airflow
* **Schedule:** Runs every 6 hours automatically (`0 */6 * * *`).
* **Pipeline Sequence:**
  ```text
  fetch_youtube_data ──> load_duckdb ──> run_dbt ──> test_dbt
  ```
* If a step fails (e.g. temporary network blip), Airflow automatically retries with exponential backoff.

### 5. Presentation: Streamlit Web Dashboard
* **KPI Cards:** Shows Total Tracked Views, Audience Engagement %, Peak Observed Speed (views/hr), Average Channel Speed (views/hr), and Total Tracked Videos.
* **Growth Curves:** An interactive Altair line chart showing views over time. Includes video multiselect filters and time horizon options (`Past 24 Hours`, `Past 7 Days`, `All Time`).
* **Momentum Bar Chart:** Shows which video is currently gaining views the fastest.
* **Performance History Table:** A clean table listing previous views, views gained, hours elapsed, and recorded timestamps.
* **Watchlist Manager:** Add any creator by their `@handle` or soft-delete them with an inline `✖` button.

---

## 📁 Project Structure

```text
creator-pulse/
├── app/
│   └── app.py                      # Streamlit interactive web dashboard
├── dags/
│   └── creator_pulse_dag.py        # Apache Airflow orchestration DAG
├── src/
│   ├── fetch_youtube.py            # YouTube API extraction & 2-unit quota logic
│   └── load_duckdb.py              # DuckDB table creator & JSONL bulk loader
├── transform/                      # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── schema.yml              # 7 automated data quality schema tests
│       ├── staging/
│       │   └── stg_video_snapshots.sql
│       └── marts/
│           ├── fct_video_velocity.sql
│           └── mart_channel_performance.sql
├── data/
│   ├── creator_pulse.duckdb        # Local embedded DuckDB analytical database
│   ├── staged/                     # Local staging area for .jsonl payload files
│   └── snowflake_backup/           # Exported raw & mart snapshot backups
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started & Local Setup

### 1. Prerequisites
* Python 3.9+
* A Google Cloud YouTube Data API v3 key (Free tier)

### 2. Environment Configuration
Create a `.env` file in the root directory:
```bash
YOUTUBE_API_KEY=your_youtube_api_key_here
```

### 3. Installation
```bash
# Clone the repository
git clone https://github.com/Darock-Dao/creator-pulse-duckdb.git
cd creator-pulse-duckdb

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Running the Dashboard
```bash
streamlit run app/app.py
```
Open **`http://localhost:8501`** in your browser.

### 5. Running dbt Transformations Manually
```bash
cd transform
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 6. Running the Airflow DAG
```bash
export AIRFLOW_HOME="$(pwd)"
airflow standalone
# Access Airflow UI at http://localhost:8080 and trigger 'creator_pulse_pipeline'
```

---

## 💡 Key Technical Highlights

1. **Point-in-Time vs. State-Over-Time:** Solves the limitation of APIs that only return lifetime totals by snapshotting changes over time to measure momentum.
2. **API Quota Optimization:** Saves **98% of API quota** by pulling from channel uploads playlists instead of expensive search queries.
3. **Sub-Hour Velocity Math:** Uses fractional seconds in `DATEDIFF` so view velocity is calculated accurately whether snapshots are hours or minutes apart.
4. **Vectorized Ingestion Over Row Inserts:** Uses DuckDB's vectorized `read_json_auto()` directly on JSON Lines payloads for instant, bulk ingestion.
5. **Idempotency & Safe Retries:** Deterministic snapshot keys combined with `QUALIFY ROW_NUMBER() = 1` prevent duplicate rows even if tasks are retried.

---

## ✅ Completed Project Roadmap

- [x] **Phase 1: YouTube API Ingestion** — Quota-optimized extraction (2 units/channel), deterministic MD5 IDs, UTC JSONL serialization.
- [x] **Phase 2: DuckDB Warehouse Setup** — Embedded OLAP database, schema, and automated table creation with watchlist auto-seeding.
- [x] **Phase 3: Python Bulk Loader** — Fast vector ingestion via `read_json_auto()` into DuckDB.
- [x] **Phase 4: dbt Transformations** — `stg_video_snapshots`, `fct_video_velocity` with fractional `LAG()` rate calculations, and 7 schema tests with `dbt-duckdb`.
- [x] **Phase 5: Airflow Orchestration** — Production DAG with 4-stage pipeline, retry backoffs, and verified execution.
- [x] **Phase 6: Streamlit GUI** — Executive KPIs, interactive Altair time-series growth curves with video filters, momentum bar chart, and soft-delete watchlist powered by local DuckDB.

---

## 📜 License
MIT License. Created by [Derek Dao](https://github.com/Darock-Dao).
