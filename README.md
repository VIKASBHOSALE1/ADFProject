# ADFProject
# Azure Data Factory — End-to-End Pipeline Project

This is a hands-on ADF project I built to understand how real-world data engineering pipelines work — from incremental data loading and REST API ingestion to dynamic routing and Delta Lake writes. Below I've documented every step I took, why I made each decision, and what each Azure resource or ADF activity actually does.

---

## Table of Contents

1. [Infrastructure Setup](#1-infrastructure-setup)
2. [Ingestion Pipeline (CDC-Based Incremental Load)](#2-ingestion-pipeline-cdc-based-incremental-load)
3. [Scheduled Pipeline + Logic App Alerting](#3-scheduled-pipeline--logic-app-alerting)
4. [REST API Pipeline (PokéAPI)](#4-rest-api-pipeline-pokéapi)
5. [Router Pipeline (Dynamic File Routing)](#5-router-pipeline-dynamic-file-routing)
6. [Schema Pipeline (Dynamic Schema Mapping)](#6-schema-pipeline-dynamic-schema-mapping)
7. [Delta Lake Pipeline (Data Flow with Upsert)](#7-delta-lake-pipeline-data-flow-with-upsert)
8. [GitHub Integration](#8-github-integration)
9. [Corrections & Things I Learned](#9-corrections--things-i-learned)

---

## 1. Infrastructure Setup

Before writing a single pipeline, I had to provision all the Azure resources that the project depends on. Everything lives under one resource group to keep things organized.

**Resource Group:** `RG-adf`

### 1.1 Storage Account — `storageadfvb`

I chose **Azure Data Lake Storage Gen2 (ADLS Gen2)** as my storage layer. The reason I picked ADLS Gen2 over plain Blob Storage is that it supports a **hierarchical namespace** — meaning I can create real directories and subdirectories inside a container, which is necessary for organizing raw, staged, and processed data properly.

Configuration choices:
- **Redundancy: LRS (Locally Redundant Storage)** — cheapest option, fine for a learning/dev environment.
- **Region: Central India** — keeping it close to reduce latency.
- **Hierarchical namespace: Enabled** — this is what makes it ADLS Gen2 instead of regular Blob Storage.

### 1.2 Data Factory — `df-projectVB`

This is the orchestration engine. Azure Data Factory (ADF) is essentially a managed ETL/ELT service. It doesn't store or compute data itself — it connects to sources and destinations and defines how data moves between them.

### 1.3 SQL Database — `sql_database`

This is my transaction source system — the place where raw operational data (like Orders) lives.

Setup:
- **SQL Server:** `sqlserver--vb`, Region: Central India
- **Authentication:** Both SQL and Microsoft Entra (so I can use admin credentials as well as Azure AD)
- **Admin Login:** `VBadmin`
- **Service Tier:** General Purpose, Serverless — auto-pauses after 30 minutes of inactivity, which saves cost significantly during development.
- **Backup Redundancy:** LRS

For networking, I enabled **Public Endpoint** access and allowed Azure services to reach the server. I also added my current client IP so I could connect from my local machine via the query editor.

---

## 2. Ingestion Pipeline (CDC-Based Incremental Load)

This is the core pipeline of the project. The problem it solves: if your source SQL database has millions of records and you've already loaded them into a data warehouse, you don't want to reload everything every time the pipeline runs. You only want to load the **new records added since the last run**. This is called **incremental data loading**.

The mechanism I used to track this is **Change Data Capture (CDC)** — specifically a JSON file (`cdc.json`) stored in ADLS Gen2 that records the date of the last successful data load.

<img width="1491" height="886" alt="image" src="https://github.com/user-attachments/assets/c74ad8fd-99b9-40b9-ad37-00ce31f15b29" />

### 2.1 Source Setup

In the Azure SQL query editor, I created a schema `source` with a table `Orders` and populated it with data.

In the storage account, I created:
- **Container:** `source`
  - **Directory:** `Monitor` — this holds the `cdc.json` file.

The `cdc.json` file starts with:
```json
{"last_load_date": "1900-01-01"}
```
Why `1900-01-01`? Because on the very first run, there's no "previous load" — so I need a date that's guaranteed to be older than any real record in the source. Using an ancient date ensures all existing records get picked up on the initial full load.

### 2.2 Linked Services

Before building the pipeline, I created **Linked Services** under the Manage tab. A linked service is simply the connection configuration between ADF and an external system — think of it as a saved credential + endpoint.

- **`ls_datalake`** — connects ADF to the ADLS Gen2 storage account.
- **`ls_sqldb`** — connects ADF to the Azure SQL Database.

### 2.3 Datasets

Datasets are pointers to specific data locations. They use linked services as their underlying connection.

- **`ds_cdc`** — points to the `cdc.json` file in the `source/Monitor/` directory.
- **`ds_sqldb`** — points to the `source.Orders` table in SQL Database.
- **`ds_parquet`** — points to the destination path in ADLS Gen2 where data will be written in Parquet format. Parquet is columnar, compressed, and much more efficient for analytics workloads than raw CSV or JSON.

### 2.4 Pipeline Activities

#### Activity 1 — Lookup (`last_cdc`)

This activity reads the `cdc.json` file and returns its contents. I need to know the `last_load_date` before I can figure out which records are "new."

Dataset used: `ds_cdc`

Output: the JSON value, which I reference in later activities as:
```
@activity('last_cdc').output.value[0].last_load_date
```

#### Activity 2 — Script (`totalresult`)

Before copying any data, I first check *how many* new records exist. There's no point triggering a copy operation if the count is zero — that would waste pipeline compute resources.
<img width="1602" height="579" alt="image" src="https://github.com/user-attachments/assets/d8a1fdaa-bb5a-4b3a-b6f5-f8c447ac21f5" />

This script runs against the SQL Database:
```sql
SELECT COUNT(*) AS newRecords
FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
WHERE last_updated > '@{activity('last_cdc').output.value[0].last_load_date}'
```

I used **pipeline parameters** (`schema` and `table`) instead of hardcoding the table name. This makes the pipeline reusable — I can pass different schema/table combinations without modifying the pipeline itself.

#### Activity 3 — If Condition (`checkNewRecords`)

This acts as a gate. It evaluates whether the count from the Script activity is greater than zero:
```
@greater(activity('totalresult').output.resultSets[0].rows[0].newRecords, 0)
```

If **true** → proceed to copy the data.
If **false** → do nothing. Pipeline ends cleanly without wasting resources.

This is important in production because pipelines often run on schedules. If there's no new data (say, a weekend with no transactions), you don't want to incur unnecessary data movement costs.

#### Activity 4 — Copy Data (inside If — True branch)

This is the actual data movement activity. It reads new records from SQL and writes them to ADLS Gen2 in Parquet format.

<img width="1678" height="700" alt="image" src="https://github.com/user-attachments/assets/9739b3d9-c8fd-4f38-869c-c75a0f1637cb" />

**Source query:**
```sql
SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
WHERE last_updated > '@{activity('last_cdc').output.value[0].last_load_date}'
```
basically it will copy the records that has added to the source after the last load date.

**Sink:** `ds_parquet` — writes to the ADLS Gen2 storage account.

#### Activity 5 — Script (`maxCDC`)

After copying, I need to update the `cdc.json` file with the most recent `last_updated` value from the data I just loaded. This becomes the new watermark for the next pipeline run.

```sql
SELECT MAX(last_updated) AS recent_update
FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
```

#### Activity 6 — Copy Data (`change_cdc`)

<img width="1707" height="901" alt="image" src="https://github.com/user-attachments/assets/97568492-4cc6-4ef7-9e2a-c1966532c6fa" />

This small copy activity writes the new max date(means recent data load date) back into `cdc.json`. It reads from an `empty.json` file (`ds_empty`, containing `{}`) and writes to `cdc.json` using the output of `maxCDC` as the content.

This is how the watermark advances — next time the pipeline runs, it picks up from this updated date instead of the old one.

### 2.5 Backdate Refresh

There's a failure scenario worth addressing: what if the pipeline silently fails for several days? When it finally runs again, the CDC date is stale — but a bulk reload would re-process all historical data, which is expensive and unnecessary.

The solution is a **backdate parameter**:
- I added a pipeline parameter called `backdate`.
- The script queries are updated to check: if `backdate` is provided, use it; otherwise fall back to the stored `last_load_date`.

<img width="1742" height="871" alt="image" src="https://github.com/user-attachments/assets/95646c6b-1598-4425-9ab5-83e8be9582ba" />

Updated Script expression:
```
@{if(empty(pipeline().parameters.backdate),
  activity('last_cdc').output.value[0].last_load_date,
  pipeline().parameters.backdate)}
```

This way, if I know the pipeline failed from `2026-01-01`, I can trigger a run with `backdate = '2026-01-01'` and it will only reload data from that date onward — not everything from 1900.

---

## 3. Scheduled Pipeline + Logic App Alerting

The ingestion pipeline above runs on-demand, but in production it needs to run on a schedule. I also want an email alert if the pipeline fails — I don't want to manually check ADF Monitor every day.

### 3.1 Scheduled Pipeline (Execute Pipeline Activity)

I created a new pipeline that wraps the ingestion pipeline using an **Execute Pipeline** activity. This pattern is useful because it separates the scheduling logic from the actual business logic. The outer pipeline only handles orchestration (when to run, what parameters to pass), and the inner pipeline handles the data work.

Configuration:
- **Activity name:** `Scheduled pipeline`
- Invokes the ingestion pipeline
- Passes schema/table parameters via dynamic content at the Execute Pipeline level

### 3.2 Logic App — Email Alerting (`RG-ADFLogicApp`)

I wanted email notifications when the pipeline fails. Instead of writing custom code, I used **Azure Logic Apps** — a no-code/low-code workflow automation service.

Steps:
1. Created a Logic App resource (Multi-tenant) under the resource group.
2. In the Logic App Designer:
   - **Trigger:** "When an HTTP request is received" — this exposes an HTTP endpoint that ADF can call.
   - **Action:** "Send an Email" — fills in recipient, subject, and body.
3. Copied the auto-generated HTTP POST URL from the trigger.

Back in the Scheduled Pipeline:
- Added a **Web Activity** named `Alert` on the failure path.
- Set the URL to the Logic App endpoint, method to `POST`, and passed the pipeline name in the body:
  ```json
  {"pipeline_name": "@{pipeline().Pipeline}"}
  ```

This way, if the pipeline fails, ADF calls the Logic App, which immediately sends an email notification.

### 3.3 Schedule Trigger

I added a **Schedule Trigger** to the Scheduled Pipeline to run it automatically at a defined frequency (daily, hourly, etc.). The trigger simply activates the outer pipeline, which in turn runs the ingestion pipeline with the appropriate parameters.

---

## 4. REST API Pipeline (PokéAPI)

This pipeline demonstrates how to ingest data from a public REST API into ADLS Gen2. I used the [PokéAPI](https://pokeapi.co/api/v2/pokemon) as the data source since it's free and has a realistic pagination structure.

The API has ~1350 records but returns only **20 records per response**. Looping manually is not the right approach in ADF — instead, I used the built-in **Pagination** feature in the Copy Activity.

### 4.1 Web Activity (`Endpoint`)

I first added a Web Activity to call `https://pokeapi.co/api/v2/pokemon` with `GET` method. This is mainly to inspect the API response structure — specifically to see the total record count, which I use later in the pagination range.

### 4.2 Copy Activity with REST Linked Service

1. **`ls_REST`** — linked service connecting ADF to the REST API endpoint.
2. **`ds_REST`** — dataset pointing to the API, linked to `ls_REST`.
3. **`ds_API`** — sink dataset in ADLS Gen2 to store the fetched data in JSON format, linked to `ls_datalake`.
   - Path: `sink/API/pokemon`

When I ran this initially, it only fetched the first 20 records. To get all records, I configured **Pagination Rules**.

### 4.3 Pagination Rules

The PokéAPI response includes a `next` field in the body containing the URL for the next page. This is called an **absolute URL** pagination style.

For this, I set:
- **Rule type:** AbsoluteUrl
- **Value:** `body.next`

ADF will keep following the `next` URL until it returns `null`, at which point it knows it has fetched all records.

For APIs that use **offset-based pagination** (no `next` URL in the response), you can instead use a `QueryParameter` rule:
- Parameter name: `offset`
- Value range: `RANGE:0:@{activity('Endpoint').output.count}:20`

And in the dataset's relative URL: `?offset={offset}` — ADF increments the offset automatically.

---

## 5. Router Pipeline (Dynamic File Routing)

This pipeline handles a common real-world scenario: multiple files arrive in a source folder, and each file needs to go to a different destination directory based on its name. Instead of hardcoding one copy activity per file, I built a fully dynamic routing pipeline.

### 5.1 Source Setup

In the `sink` container, I created a directory called `files` and uploaded three CSV files: `customers.csv`, `locations.csv`, `drivers.csv`.

### 5.2 Dynamic Dataset — `ds_csv_dynamic`

Rather than creating a separate dataset for each file, I created one reusable dataset with three parameters:

| Parameter | Purpose |
|-----------|---------|
| `p_container` | Which container |
| `p_directory` | Which directory within the container |
| `p_files` | Which specific file |

In the dataset's file path, each segment uses the corresponding parameter as dynamic content:
```
@dataset().p_container / @dataset().p_directory / @dataset().p_files
```

This means a single dataset can point to any file in any location — no need to create multiple datasets as the project scales.

### 5.3 Pipeline Activities

**Get Metadata Activity:**
- Dataset: `ds_meta` — points to `sink/files/`
- Field list: `childItems` — returns a list of all files and folders in that directory.

I needed this because the pipeline doesn't know in advance how many files exist. The metadata activity discovers them dynamically.

**ForEach Activity:**
- Items: `@activity('Get Metadata').output.childItems`
- This iterates over every item returned by Get Metadata — so if there are 3 files, it runs 3 times.

**Switch Activity (inside ForEach):**
- Expression: `@split(item().name, '.')[0]`
  - This splits the file name on the dot and takes the first part — so `customers.csv` becomes `customers`.
- Cases: `customers`, `drivers`, `locations`
- Each case has a Copy Activity that reads from `source/files/@{item().name}` and writes to the appropriate subdirectory in the `sink` container.

### 5.4 Validation Activity

Added at the very start of the pipeline. This activity waits for a specific file to appear at a given path before proceeding. It acts as a **storage event gate** — the pipeline only continues once the expected file is actually present.

Configuration:
- Dataset: `ds_csv_dynamic`
- Path: `source/trigger/locations.csv`

If the file isn't there yet, the activity keeps polling (with a configurable timeout) until it appears.

### 5.5 Delete Activity

At the end of the pipeline, after all files have been copied to their destinations, I added a **Delete Activity** to remove the original files from the source directory. This prevents the source folder from accumulating processed files indefinitely, which would unnecessarily increase storage costs and cause confusion on the next pipeline run.

**Final Router Pipeline structure:**
`Validation → Get Metadata → ForEach [Switch → Copy] → Delete`

---

## 6. Schema Pipeline (Dynamic Schema Mapping)

When copying CSV files to a destination, ADF needs to know the column structure (schema) of each file so it can map source columns to destination columns correctly. If each file has a different schema (different column names/types), you can't use a single static mapping.

This pipeline solves that by storing each file's schema as a **pipeline parameter** and applying the correct one dynamically during the copy.

### 6.1 Schema Parameters

I created three pipeline-level parameters of type `Object`:

| Parameter | Points to |
|-----------|----------|
| `customers_schema` | Schema of `customers.csv` |
| `drivers_schema` | Schema of `drivers.csv` |
| `locations_schema` | Schema of `locations.csv` |

To get each schema value:
1. Created a temporary Copy Activity pointing to the file.
2. In the Mapping tab, clicked **Import Schema** — ADF reads the file and generates a mapping JSON.
3. Opened the activity's source code and copied the `translator` JSON block.
4. Pasted it as the default value for the corresponding parameter.

This was repeated for each file.

### 6.2 Pipeline Activities

**Get Metadata Activity (`get_schema`):**
- Returns `childItems` from the `source/files/` directory — same as the Router pipeline.

**ForEach Activity:**
- Iterates over each file discovered by Get Metadata.

**Copy Activity (inside ForEach):**
- Source: `ds_csv_dynamic` with `p_files = @item().name` — picks up whichever file the loop is currently on.
- Sink: `ds_csv_dynamic` with `p_container = sink`, `p_directory = schema`, `p_files = @item().name`.
- Mapping tab — instead of a static mapping, I added dynamic content:

```
@if(equals(item().name, 'customers.csv'), pipeline().parameters.customers_schema,
  if(equals(item().name, 'drivers.csv'), pipeline().parameters.drivers_schema,
    pipeline().parameters.locations_schema))
```

This expression checks the current file name and applies the correct schema parameter. Each file gets its own mapping without me having to create separate copy activities.

**Final Schema Pipeline structure:**
`Get Metadata → ForEach [Copy Schema]`

---

## 7. Delta Lake Pipeline (Data Flow with Upsert)

All previous pipelines used **Copy Activity**, which is a straightforward data movement tool — source to sink, no transformation logic. But what if I want to **transform** the data or implement **upsert logic** (insert if new, update if existing)?

That's where **Data Flows** come in. Data Flows are Spark-based transformation engines inside ADF. They allow you to define transformation steps visually.

### 7.1 Data Flow Activities

**Source (`deltasource`):**
- Dataset: `ds_csv_dynamic` — reads the CSV files.
- **Projection tab:** I clicked **Import Projection** to let ADF infer the column types from the file. This is important — without it, all columns default to string type.
- **Data Flow Debug:** I enabled this before running the flow so I could preview data at each step without triggering a full pipeline run.

**Select Cols (`selectCols`):**
- This step filters the columns — I only pass forward the ones I actually need for the destination. In my case: `location_id`, `city`, `state`, `country`, `last_updated_timestamp`.
- Keeping only relevant columns reduces the data volume going into the sink and avoids writing unnecessary fields.

**Alter Rows (`alterRows`):**
- This is where I define the **row-level operation**. I set the condition to **Upsert** — meaning:
  - If a row with the same key already exists in the destination → **update** it.
  - If it doesn't exist → **insert** it.
- This is critical for incremental pipelines because source records can be updated after their initial load. A plain insert would create duplicates; upsert handles both cases cleanly.

**Sink (`deltasink`):**
- **Format:** Delta Lake (inline dataset type: Delta)
- **Path:** `sink/Deltasink`
- **Update method:** Allow Upsert only (matching what I set in Alter Rows).
- **Key column:** `location_id` — ADF uses this to determine if an incoming row matches an existing one.

Why Delta format? Delta Lake stores data in Parquet files but adds a transaction log on top. This gives you ACID transactions, time travel (query previous versions of data), and efficient upsert support — none of which plain Parquet provides.

---

## 8. GitHub Integration

To version-control all pipeline definitions, datasets, and linked services, I connected ADF to a GitHub repository.

Steps:
1. Go to the **Manage** tab in ADF Studio.
2. Enable **Git configuration**.
3. Authenticate with GitHub and select the repository.
4. ADF saves all pipeline JSON definitions to the repo on each publish/commit.

The repository structure ADF creates:

```
ADFProject/
├── dataflow/
├── dataset/
├── factory/
├── linkedService/
├── pipeline/
├── trigger/
├── README.md
└── publish_config.json
```

Every pipeline, linked service, dataset, trigger, and data flow is stored as a JSON file. This means the entire ADF project is reproducible — anyone can clone the repo, import it into a new ADF instance, and have the same setup.

---

## 9. concepts


**Bulk load vs Backdate Refresh:** Bulk load (reloading everything from scratch) is expensive and slow. Backdate Refresh is the right answer when a pipeline fails silently for a period — pass in the date from which you want to reload and the pipeline will catch up only from that point.

**Why `empty.json` in the Change CDC step:** I needed an ADF Copy Activity to write a new value to `cdc.json`. Copy Activity always needs a source dataset. Since there's nothing to actually copy *from* at this point, I used an empty JSON file (`{}`) as a dummy source, and the real content comes from the dynamic expression in the sink. A bit of a workaround, but it's the standard pattern for updating metadata files via ADF Copy Activity.

**Data Flow vs Copy Activity:** Copy Activity = move data, no transformation. Data Flow = move + transform, with full Spark compute behind it. Data Flows cost more to run but are necessary for anything beyond simple column mapping.

---

## Resources

- [Azure Data Factory Documentation](https://learn.microsoft.com/en-us/azure/data-factory/)
- [ADLS Gen2 Overview](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Delta Lake on Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/)
- [PokéAPI](https://pokeapi.co/)
