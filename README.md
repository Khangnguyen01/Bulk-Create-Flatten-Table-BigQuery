# BigQuery Event Flattener – Script Guide 🧰⚡

Create multiple **flattened event tables** on BigQuery in minutes — based on your **GA4 intraday** data — using the provided Python script:  
**`Create Flatten Table BigQuery.py`**

> This README is generated specifically for the uploaded script. It documents the prompts, behavior, generated outputs, assumptions, and gotchas you should know before running it.

---

## ✨ What the script does

Depending on your input, the script runs in **two pathways**:

1) **No clustered source table available (choose `0`)** → it **creates a clustered & partitioned raw source table** from `events_intraday_*` within your date window.  
2) **Clustered source table already exists (choose `1`)** → it **discovers events**, extracts their **parameters**, and **generates SQL** to:
   - Create one **flattened table per event** (partitioned by `event_date`, clustered by `version, country, platform`)
   - Create a **`user_fact`** table
   - Create **incremental update** SQL for appending new records

The script also writes the SQL files into your chosen project folder so you can review and run them separately.

---

## 📦 Requirements

- **Python** 3.9+ recommended
- Packages: `google-cloud-bigquery`, `pandas`, `numpy`
- **GCP** Project with BigQuery enabled
- Permissions to read from your **raw GA4 dataset** and write to your **working dataset**

Install packages:
```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install google-cloud-bigquery pandas numpy
```

Authenticate (choose one):
```bash
# User credentials
gcloud auth application-default login

# Or, service account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

---

## 🧭 Interactive prompts (exact text)

When you run the script, you’ll be asked the following (in order):

1. `Enter your dataset ID:` → e.g., `my_project`
2. `Enter your schema ID:` → e.g., `analytics_123456`
3. `Enter your intraday(e.g: 20250805):` → **YYYYMMDD**
4. `Enter start date for your data(e.g: 20250721):` → **YYYYMMDD**
5. `Enter your project folder to save queries:` → path (relative to script; files write to `../<folder>/`)
6. `Enter your clustered source table schema ID:` → e.g., `analytics_work`
7. `Enter your clustered source table :` → e.g., `events_intraday_clustered`
8. `Do you have a clustered source table? (0/1):` → **0** (create) or **1** (use existing)

> Tip: The script builds `TABLE_ID = "events_intraday_<INTRADAY>"` internally from the `INTRADAY` you provide.

---

## 🏗️ Pathway 1 — Create clustered source table (answer `0`)

- Builds `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}` from `events_intraday_*` within `[START_DATE, END_DATE)`  
- **Partitions** by `event_date` and **clusters** by `event_name, version, country, platform`
- Persists SQL to:  
  `../<PROJECT_FOLDER>/source_table_clustered.sql`

Example of the generated DDL pattern:
```sql
CREATE SCHEMA IF NOT EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}`;
DROP TABLE IF EXISTS `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`;
CREATE OR REPLACE TABLE `{RAW_DATASET_ID}.{RAW_SCHEMA_ID}.{SOURCE_TABLE_ID}`
PARTITION BY event_date
CLUSTER BY event_name, version, country, platform AS
SELECT
  user_pseudo_id, event_name,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_timestamp, event_value_in_usd,
  app_info.version, geo.country, geo.continent, geo.region, geo.sub_continent,
  platform, device.mobile_brand_name, device.mobile_model_name,
  device.advertising_id, device.time_zone_offset_seconds,
  user_ltv.revenue AS ltv_revenue,
  event_params
FROM `{DATASET_ID}.{SCHEMA_ID}.events_intraday_*`
WHERE _TABLE_SUFFIX >= '{START_DATE}' AND _TABLE_SUFFIX < '{END_DATE}';
```

---

## 🧪 Pathway 2 — Use existing clustered source (answer `1`)

### 1) Event discovery
- Reads distinct `event_name` from `{DATASET_ID}.{SCHEMA_ID}.events_intraday_<INTRADAY>`
- Drops events that end with digits (e.g., suffix `_123`) and a small list of default Firebase events
- For each remaining event, samples its `event_params` at the given `INTRADAY` date to detect available keys and value types

### 2) Generated outputs
- **Create SQL** (per-event flattened tables + `user_fact`):  
  `../<PROJECT_FOLDER>/create_flatten_table.sql`
- **Update SQL** (incremental inserts for events + `user_fact` + refresh source by 2-day window):  
  `../<PROJECT_FOLDER>/update_raw_table.sql`

### 3) Flattened table pattern (per event)
- **Partition** by `event_date`  
- **Cluster** by `version, country, platform`  
- Common columns include: `user_pseudo_id, event_date, event_timestamp, version, country, platform`  
- Plus dynamically extracted columns from `event_params` with appropriate type casting  
- Special cases:
  - `in_app_purchase` → adds `event_value_in_usd`
  - `first_open` → adds `advertising_id`, `time_zone_offset_seconds`

### 4) `user_fact` table pattern
- Creates a partitioned/clustered user fact table including geo/device fields and GA session keys:
  - `ga_session_id` and `ga_session_number` are pulled via subselects from `event_params`

### 5) Incremental updates
- For each event table: appends rows where `event_timestamp` is **greater than** target table’s `MAX(event_timestamp)` and **less than** today’s midnight (UTC truncation)  
- Updates `user_fact` similarly  
- Refreshes the **source clustered table** with the last **2 days** of `events_intraday_*` not yet present

---

## 🚀 How to run

> On macOS/Linux (note the filename contains spaces — wrap in quotes):  
```bash
python "Create Flatten Table BigQuery.py"
```

> On Windows (PowerShell):  
```powershell
python ".\Create Flatten Table BigQuery.py"
```

Then follow the interactive prompts listed above.

---

## 🗂️ Files the script writes

- `../<PROJECT_FOLDER>/source_table_clustered.sql` *(Pathway 1)*
- `../<PROJECT_FOLDER>/create_flatten_table.sql` *(Pathway 2)*
- `../<PROJECT_FOLDER>/update_raw_table.sql` *(Pathway 2)*

> The `..` means: relative to the directory where you run the script, files are written one level **up** into `<PROJECT_FOLDER>`. Ensure that folder exists or adjust paths as needed.

---

## 🔧 Customization knobs

- **Partition/Cluster columns**: Adjust in the SQL string builders if you need different strategies
- **Event exclusions**: Tweak `DEFAULT_EVENTS_FIREBASE` to include/exclude global events
- **Param white/blacklists**: Modify how parameter keys are detected and typed
- **Windowing**: Change the 2-day refresh and `event_timestamp` filters in the update SQL if your ingestion latency differs
- **Naming**: Use a different `{SOURCE_TABLE_ID}` or target dataset/schema to segment environments

---

## 💡 Tips & Best Practices

- Keep a small **lookback** window when doing incremental loads to avoid late-arriving events
- Always **filter by partition** (`event_date`) when querying flattened tables
- Consider **clustering** by fields frequently used in filters/joins (e.g., `version`, `country`, `platform`, `user_pseudo_id` if added)
- Validate event parameter types; strings that look numeric are cast to `INT64` by the script

---

## 🧯 Troubleshooting

- **Permission denied** → Verify BigQuery roles (`dataViewer` on source, `dataEditor` on target, `jobUser` on project)
- **No such dataset/table** → Create the datasets first or confirm IDs in prompts
- **Empty outputs** → Check your `INTRADAY` date actually has data in `events_intraday_*`
- **Paths invalid** → Confirm `../<PROJECT_FOLDER>/` exists and you have write permissions
- **Schema mismatch on `user_fact` update** → Ensure the INSERT column list matches the CREATE schema (see **Known notes**)

---

## 📝 Known notes (read before production)

- **Event name variants**: The script treats `in_app_purchase` with a special column, but the default exclusion list includes `in_app_purchased`. Ensure your actual event names match your GA4 export.
- **Default param list**: If a parameter should be excluded but still appears, review `DEFAULT_PARAMS_FIREBASE` and add a comma where needed.  
- **User Fact update**: The initial CREATE includes more columns (e.g., `ltv_revenue`, `continent`, `sub_continent`) than the INSERT in the update step. Align them before running incremental updates in production.
- **Timezones**: `event_timestamp` is microseconds since epoch (UTC). Partitioning uses a date derived from that timestamp.

---

## 📄 License

Choose a license that fits your needs (MIT recommended for internal tooling).

---

## 🙋 FAQ

**Q: Can I run this daily?**  
Yes. Use Pathway 2 daily after the initial build; the update SQL appends new data based on `event_timestamp` and a rolling 2-day refresh for the source table.

**Q: Where do I edit which params are materialized?**  
Inside the script’s event parameter detection and SQL builders (search for `events_params_dict` logic).

**Q: Can I change the output folder?**  
Yes, the prompt `Enter your project folder to save queries:` controls the folder name; path is currently prefixed with `../` relative to the script run directory.
