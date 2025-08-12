# BigQuery Bulk Flattener 🧰⚡
Create **n** flattened tables for **n** events in minutes — not hours.

> A small Python utility that helps data analysts auto‑generate flattened BigQuery tables (partitioned & clustered) for each event in your app analytics stack.

---

## ✨ Why this exists
Working analysts often need to build a *separate flattened table per event* (e.g., `purchase`, `signup`, `level_up`). Doing this by hand means copy‑pasting `UNNEST` blocks, wiring partitions, adding dedupe logic, and keeping naming conventions consistent. This tool automates the boring parts so you can focus on **analysis**, not plumbing.

---

## 🧭 TL;DR
```bash
# Replace bulk_flatten.py with your actual filename
python bulk_flatten.py \
  --project my-gcp-project \
  --source-dataset raw_events \
  --target-dataset analytics_flat \
  --events purchase,signup,level_up \
  --mode incremental \
  --partition-by event_date \
  --cluster-by user_pseudo_id,event_name \
  --dedupe-key event_id
```

- Creates or updates one flattened table **per event** under `analytics_flat`.
- Uses **partitioning** (by date) and optional **clustering** (e.g., by `user_pseudo_id,event_name`).
- **Incremental** mode appends only new data based on a watermark (e.g., max `event_timestamp`).

> If your script’s flags differ, adjust names accordingly — this README is a professional template you can tailor.

---

## 📦 Features
- **Bulk generation**: One run → multiple flattened event tables.
- **Full or incremental** builds (watermark on `event_timestamp`).  
- **Idempotent** writes: re‑running won’t create duplicates (via `QUALIFY ROW_NUMBER()`).
- **Partition & cluster** knobs for cost + performance.
- **Consistent naming**: `target_dataset.event_<name>_flat` (customizable).
- **Dry‑run** & **preview** support for safer deployments (if implemented).

---

## 🏗️ Assumptions
- Raw event tables contain nested/repeated fields (e.g., `event_params`, `user_properties`).
- There’s a timestamp column such as `event_timestamp` (microseconds) and an optional unique key (e.g., `event_id` or `user_pseudo_id + event_timestamp + event_name`).

Adjust these to match your actual schema.

---

## 🔑 IAM & Auth
**Minimum recommended roles** for the service account / identity running the job:
- `roles/bigquery.dataViewer` on **source** dataset
- `roles/bigquery.dataEditor` on **target** dataset
- `roles/bigquery.jobUser` on the **project**

**Authenticate**
```bash
# Option 1: User credentials
gcloud auth application-default login

# Option 2: Service account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"
```

---

## 🧰 Installation
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U google-cloud-bigquery google-cloud-core python-dateutil pyyaml
```

Project layout (suggested):
```
your-repo/
├─ bulk_flatten.py
├─ config.yaml
└─ README.md
```

---

## ⚙️ Configuration

### CLI flags (suggested)
| Flag | Required | Example | Notes |
|---|---|---|---|
| `--project` | ✅ | `my-gcp-project` | GCP project id |
| `--source-dataset` | ✅ | `raw_events` | Where raw events live |
| `--target-dataset` | ✅ | `analytics_flat` | Where flattened tables are written |
| `--events` | ✅ | `purchase,signup` | Comma‑separated list of event names |
| `--mode` |  | `full` \| `incremental` | Default `full` |
| `--start-date` |  | `2025-07-01` | Optional backfill window (YYYY‑MM‑DD) |
| `--end-date` |  | `2025-08-01` | Optional end of window |
| `--partition-by` |  | `event_date` | Date column to partition by |
| `--cluster-by` |  | `user_pseudo_id,event_name` | Comma‑separated columns |
| `--dedupe-key` |  | `event_id` | Unique identifier; see **Dedupe** |
| `--dry-run` |  |  | Print SQL without executing |

### YAML config (optional)
```yaml
project: my-gcp-project
source_dataset: raw_events
target_dataset: analytics_flat

events:
  - purchase
  - signup
  - level_up

mode: incremental                 # full | incremental
window:
  start: 2025-07-01               # optional
  end: 2025-08-01                 # optional

partition_by: event_date
cluster_by: [user_pseudo_id, event_name]

dedupe:
  key: event_id
  strategy: keep_latest           # uses ORDER BY event_timestamp
  order_by: event_timestamp
```

Run with:
```bash
python bulk_flatten.py --config config.yaml
```

---

## 🧪 Example SQL pattern (generated per event)

> Example for `purchase` (simplified). Your script will render something similar.

```sql
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.analytics_flat`;

CREATE OR REPLACE TABLE `my-gcp-project.analytics_flat.event_purchase_flat`
PARTITION BY DATE(event_ts)
CLUSTER BY user_pseudo_id, event_name AS
SELECT
  DATE(TIMESTAMP_MICROS(e.event_timestamp)) AS event_date,
  TIMESTAMP_MICROS(e.event_timestamp)       AS event_ts,
  e.event_name,
  e.user_pseudo_id,
  -- Flattened event parameters
  p.key                                      AS param_key,
  COALESCE(p.value.string_value,
           CAST(p.value.int_value  AS STRING),
           CAST(p.value.float_value AS STRING),
           CAST(p.value.double_value AS STRING)) AS param_value_str,
  -- Example of pulling specific params
  MAX(IF(p.key = 'transaction_id', p.value.string_value, NULL)) AS transaction_id,
  MAX(IF(p.key = 'value',          p.value.double_value, NULL)) AS value
FROM `my-gcp-project.raw_events.events_*` AS e
LEFT JOIN UNNEST(e.event_params) AS p
WHERE e.event_name = 'purchase'
  -- Optional incremental window
  AND DATE(TIMESTAMP_MICROS(e.event_timestamp)) BETWEEN @start_date AND @end_date
GROUP BY 1,2,3,4,5,6;
```

### Dedupe (recommended)
If your source can contain duplicates, add a dedupe step. Two common patterns:

**A. Unique event id**
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) = 1
```

**B. Composite key**
```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY user_pseudo_id, event_name, event_timestamp
  ORDER BY ingestion_time DESC
) = 1
```

---

## 🚀 Running

### Full rebuild
```bash
python bulk_flatten.py \
  --project my-gcp-project \
  --source-dataset raw_events \
  --target-dataset analytics_flat \
  --events purchase,signup,level_up \
  --mode full
```

### Incremental append (watermark)
```bash
python bulk_flatten.py \
  --project my-gcp-project \
  --source-dataset raw_events \
  --target-dataset analytics_flat \
  --events purchase,signup,level_up \
  --mode incremental
```

> Store the **max processed timestamp** in a metadata table (e.g., `analytics_flat._metadata`) so each run only processes new rows.

---

## 🗂️ Naming convention
Default (suggested):
```
{target_dataset}.event_{event_name}_flat
# e.g. analytics_flat.event_purchase_flat
```

Override via `--table-template` if your script supports it (e.g., `"{dataset}.{event}_flat_v2"`).

---

## 💸 Performance & Cost Tips
- Always **partition** by a date derived from `event_timestamp` and **filter by partition** in queries.
- **Cluster** by high‑cardinality fields often used in filters/joins (e.g., `user_pseudo_id`).
- Avoid `SELECT *` from nested arrays; **UNNEST only what you need**.
- For backfills, run by **daily batches** to keep slot usage predictable.
- Prefer **`CREATE OR REPLACE TABLE AS SELECT`** over write‑truncates in production pipelines.

---

## ⏱️ Scheduling (optional)

### Cloud Composer / Airflow
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("bq_bulk_flatten",
         start_date=datetime(2025, 8, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    run = BashOperator(
        task_id="flatten",
        bash_command=(
            "source /path/to/venv/bin/activate && "
            "python /opt/pipelines/bulk_flatten.py "
            "--config /opt/pipelines/config.yaml"
        ),
    )
```

### Cloud Scheduler → Cloud Run / VM
Containerize the script and trigger via a scheduled HTTP request.

---

## 🪪 Telemetry & Logging
- Log **one line per event table** with rows processed + duration.
- Emit a final summary (tables created, skipped, failed).
- Optional: write run stats to `analytics_flat._runs` with `run_id`, `started_at`, `ended_at`, `mode`, `watermark_max`.

---

## 🧯 Troubleshooting
- **Permission denied** → Check IAM roles on datasets + `bigquery.jobUser` at project level.
- **Query cost too high** → Verify partition filters, reduce `UNNEST`, cluster wisely.
- **Duplicates** → Ensure `dedupe-key` or `QUALIFY ROW_NUMBER()` is applied.
- **Incremental misses rows** → Watermark column must be **monotonic** and in UTC; include a small **lookback window** (e.g., 2–6 hours).

---

## 🧭 Roadmap (suggested)
- ✅ Basic bulk generation
- ✅ Incremental mode with metadata watermark
- ⏳ Schema evolution handling (`REPLACE` on new columns)
- ⏳ Automatic param extraction via allow/deny lists
- ⏳ Jinja templates for custom per‑event SQL

---

## 🤝 Contributing
PRs welcome! Please open an issue describing the use‑case first. Follow conventional commits and include before/after query plans where relevant.

---

## 📄 License
MIT (or your preferred license).

---

## 🙋 FAQ
**Q: My events are in one giant table (`events_*`). Can this still work?**  
Yes — filter by `event_name` per target table and add `UNNEST` as needed.

**Q: Where should I store the watermark?**  
A small metadata table under the target dataset is convenient (e.g., `analytics_flat._metadata`).

**Q: Can I run per‑day backfills?**  
Absolutely. Provide `--start-date`/`--end-date` or drive via Airflow with a daily execution date.
