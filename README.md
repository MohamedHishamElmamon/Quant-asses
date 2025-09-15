# DealApp Real Estate Data Pipeline

End-to-end pipeline for Saudi real-estate analytics: **web scraping → ADLS Gen2 → Fabric (Bronze/Silver/Gold) → Power BI (Direct Lake)**. Supports **manual uploads** and **CI/CD** for both the scraper (Azure Functions in Docker) and Fabric items (Git + Deployment Pipelines).

<img width="977" height="427" alt="Arch" src="https://github.com/user-attachments/assets/325597ce-dfcd-4a60-8726-ba7141e83976" />


> **Assessment coverage:** bedroom price uplift (2→3 BR by households), time-to-close & drivers, normalized correlations, and 6% cap-rate valuation with meter-price distributions.&#x20;

---

## Table of Contents

* [Project Overview](#project-overview)
* [Architecture](#architecture)
* [Web Scraping](#web-scraping)
* [Data Pipeline (Medallion)](#data-pipeline-medallion)
* [Analytics & Power BI](#analytics--power-bi)
* [Setup & Deployment](#setup--deployment)
* [Security, Identity & Monitoring](#security-identity--monitoring)
* [Output Schemas](#output-schemas)
* [Troubleshooting](#troubleshooting)
* [Future Enhancements](#future-enhancements)

---

## Project Overview

**Purpose & Scope.** This repo implements a market-analytics pipeline that:

* Scrapes listings from **DealApp** using a token-aware browser automation approach.
* Ingests **manual CSV/Excel** uploads from users.
* Lands raw data in **Azure Data Lake Storage Gen2 (ADLS Gen2)** and exposes it to **Microsoft Fabric** with **OneLake Shortcuts** . 
* Curates data in **Fabric Warehouse** via **`COPY INTO`** (Silver) and models a **Gold star schema** for BI. 
* Publishes **Power BI** with **Direct Lake** for high-performance analytics over Delta in OneLake. 

---

## Architecture


<img width="977" height="427" alt="Arch" src="https://github.com/user-attachments/assets/965e31b7-5b01-4ce4-b008-1925d8be4877" />

**Why these components**

* **Functions in containers (Linux)** let us ship the scraper and its dependencies predictably; containerized Functions are supported and easily deployed from a registry (Premium/Dedicated or ACA hosting). 
* **Timer trigger** schedules the crawl; **Event Grid trigger** handles “BlobCreated” for manual uploads. 
* **ADLS Gen2** provides a Hadoop-compatible filesystem, hierarchical namespace, and Entra-based ACLs for analytics at scale. 
* **OneLake Shortcuts** point Fabric directly to ADLS Gen2 paths (zero-copy exposure of raw). 
* **Warehouse `COPY INTO`** is the **primary** high-throughput ingest method into Fabric tables. 
* **Direct Lake** delivers in-memory performance over OneLake Delta for BI. 
* **Fabric ALM:** Git integration + Deployment Pipelines for Dev→Test→Prod promotion. 

---

## Web Scraping

**Approach (high level).**

* **Playwright** launches a headless browser, navigates DealApp, and captures auth headers from network requests; **Requests** replays API calls using harvested tokens with rotation/backoff. (Playwright is a Microsoft-maintained, multi-browser automation SDK.) 
**Repo layout**

```
dealapp-scraper/
├─ src/
│  ├─ scrapers/
│  │  ├─ base_scraper.py
│  │  └─ dealapp_scraper.py
│  ├─ utils/
│  │  ├─ token_harvester.py
│  │  ├─ property_parser.py
│  │  └─ file_handler.py
│  ├─ config/
│  │  ├─ settings.py
│  │  └─ constants.py
│  └─ models/property.py
├─ logs/    ├─ data/
├─ Dockerfile  ├─ docker-compose.yml  ├─ requirements.txt  └─ main.py
```

---

## Data Pipeline (Medallion)

**Partitioning & landing.** Raw files written to ADLS Gen2 under:

```
abfss://realestate@<account>.dfs.core.windows.net/raw/source=<dealapp|manual>/ingest_date=YYYY-MM-DD/...
```

### Bronze (External over raw)

* Exposed via **OneLake Shortcut** from Fabric Lakehouse/SQL endpoint. Don’t mutate; treat as immutable append-only.

### Silver (Internal, curated)

* Ingest from ADLS using **`COPY INTO`** (CSV/Parquet) with rejected-rows path; CTAS/SQL for type casting, dedup, validation, standardization.

### Gold (Internal, BI-ready)

* Star schema (facts/dims) with calculated metrics and date dimension; optimized for Direct Lake consumption. 
**Example (DDL snippets)**

```sql
-- External 'Bronze' over raw (illustrative)
CREATE EXTERNAL TABLE bronze.scraped_properties_external (..., file_date DATE)
WITH ( LOCATION='/raw/source=dealapp/', DATA_SOURCE = AzureDataLakeStorage, FILE_FORMAT = CSVFormat );

-- Silver
CREATE TABLE silver.properties (
  property_id BIGINT IDENTITY(1,1),
  source_system VARCHAR(20), source_id VARCHAR(50),
  property_type NVARCHAR(100), listing_type VARCHAR(20),
  city NVARCHAR(100), district NVARCHAR(100),
  price DECIMAL(18,2), area DECIMAL(10,2), bedrooms INT,
  is_current BIT DEFAULT 1, insert_timestamp DATETIME DEFAULT GETDATE()
);

-- Gold (example metrics table)
CREATE TABLE gold.property_analytics (
  date_key INT, city NVARCHAR(100), district NVARCHAR(100), property_type NVARCHAR(100),
  avg_price DECIMAL(18,2), avg_price_per_sqm DECIMAL(18,2), total_properties INT
);
```

---

## Analytics & Power BI

<img width="1587" height="918" alt="PBI2" src="https://github.com/user-attachments/assets/370805e9-7487-4e33-b0f6-07418cc89303" />
<img width="1591" height="915" alt="PBI1" src="https://github.com/user-attachments/assets/e100dfdf-7f6c-4bb9-a7d2-eed0c22e1aba" />

**What’s included (per assessment):**

* **2BR→3BR uplift** for **families** and **singles** in Riyadh.
* **Average time-to-close** + drivers; fastest **villa** district.
* **Normalized correlation matrix** and commentary.
* **6% cap-rate valuation** (rent→sale) and **meter-price distributions** by type/district.&#x20;

**Core formulas**

* **Bedroom premium (%):** $(AvgPrice_{3BR}-AvgPrice_{2BR})/AvgPrice_{2BR}\times100$
* **Average Days to Close:** `DATEDIFF([post_date],[close_date],DAY)` (DAX reference).
* **Pearson correlation** on z-scored fields; interpret r∈\[−1,1] as linear strength/direction.
* **Cap-rate valuation:** $ \text{Value}\approx\frac{\text{Annual NOI}}{\text{Cap Rate}}$ → 6% used here.

**Direct Lake** is used for the semantic model to keep reports responsive on large Delta tables in OneLake.

---

## Setup & Deployment

### Prerequisites

* Azure subscription with: **Storage (ADLS Gen2)**, **Function App (Premium/Dedicated)**, **ACR**, **Application Insights**, **Key Vault**; Microsoft **Fabric workspace**. 
* Docker & Python 3.10+.
* Power BI Desktop (optional for local modeling).

### Local dev

```bash
git clone <repo>
cd dealapp-scraper
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env  # fill values
python main.py
```

### Environment variables (example)

```
STORAGE_ACCOUNT_NAME=...
STORAGE_FILESYSTEM=realestate
RAW_CONTAINER_PATH=raw
DEALAPP_BASE_URL=https://dealapp.sa
SCRAPE_CONCURRENCY=4
```

### Docker & Azure Functions (container)

```bash
# Build & run
docker build -t dealapp-scraper .
docker-compose up
# Push to ACR and point Function App at the image
az acr build -r <acrName> -t dealapp-scraper:latest .
az functionapp config container set -g <rg> -n <funcName> \
  --container-image-name <acrName>.azurecr.io/dealapp-scraper:latest
```

### GitHub Actions (CI/CD)

**Build & push image to ACR**

```yaml
name: build-and-push-scraper
on: { push: { branches: [ main ] } }
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: azure/login@v2
        with: { creds: ${{ secrets.AZURE_CREDENTIALS }} }
      - run: az acr login -n ${{ secrets.ACR_NAME }}
      - run: |
          IMAGE=${{ secrets.ACR_NAME }}.azurecr.io/dealapp-scraper:${{ github.sha }}
          docker build -t $IMAGE .
          docker push $IMAGE
```

**Swap the Function App image**

```yaml
name: deploy-function
on: { workflow_run: { workflows: ["build-and-push-scraper"], types: [completed] } }
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: azure/login@v2
        with: { creds: ${{ secrets.AZURE_CREDENTIALS }} }
      - run: |
          az functionapp config container set \
            -g ${{ secrets.RESOURCE_GROUP }} \
            -n ${{ secrets.FUNC_NAME }} \
            --container-image-name ${{ secrets.ACR_NAME }}.azurecr.io/dealapp-scraper:${{ github.sha }}
          az functionapp restart -g ${{ secrets.RESOURCE_GROUP }} -n ${{ secrets.FUNC_NAME }}
```

### Fabric ALM (analytics CI/CD)

* **Connect workspace to Git** (Fabric Git Integration), then use **Deployment Pipelines** for Dev→Test→Prod promotion.

---

## Security, Identity & Monitoring

* **Managed Identity + RBAC**: grant the Function App MI **Storage Blob Data Contributor** on the storage account/container to write raw files.
* **Key Vault references**: keep secrets in Key Vault, reference them in Functions **app settings** (no code changes). 
* **Event Grid filter**: when reacting to manual uploads on ADLS Gen2, filter on **`FlushWithClose`** so triggers fire only after a blob is fully committed. 
* **Application Insights**: Functions integrates natively; use it to monitor executions, dependencies, and failures. 

---

## Output Schemas

**Scraped CSV**

```
type,listing_type,city,district,district_en,price,price_numeric,area,area_numeric,bedrooms,ad_id,code,title,lat,lng,created_at,source,extraction_date
"شقة للبيع","sale","الرياض","العليا","Al Olaya","1,500,000",1500000,"150",150,3,"abc123","CODE123","شقة فاخرة للبيع",24.7136,46.6753,"2024-01-15T10:30:00","DealApp API","2024-01-15T12:00:00"
```

**Manual input (CSV)**

```
Advertisement Number,User Number,Creation Time,Last Update Time,Property Type,Price,Area Dimension,Number of Bedrooms,district_name_ar,district_name_en,Latitude,Longitude
121329,15735,10/5/2016 21:13,10/5/2016 21:13,شقة,460000,175,3,ظهرة لبن,Dhahrat Laban,24.6266,46.5492
```

**Unified Silver (high level)**

* Standardized types (numeric price/area), normalized categories, SCD-style `is_current`, audit fields (`insert_timestamp`, `update_timestamp`).

---

## Troubleshooting

* **Scraper tokens**: site layout changes can break interception—re-record selectors; increase waits/retries (Playwright auto-waits help). 
* **Event Grid fires twice**: ensure **FlushWithClose** filtering for ADLS Gen2. 
* **Slow visuals**: verify **Direct Lake** and avoid many-to-many/bidirectional relationships; pre-aggregate where sensible. 

---

## Future Enhancements

* Additional sources (other portals, registries), macro indicators.
* ML price prediction, anomaly detection.
* Real-time streaming, self-healing DQ rules.

---

## Appendix

### Key DAX snippets

```DAX
-- Avg days to close
Avg Days to Close :=
AVERAGEX(
  FILTER(VALUES(Properties[ad_id]), NOT(ISBLANK(Properties[post_date])) && NOT(ISBLANK(Properties[close_date]))),
  DATEDIFF(Properties[post_date], Properties[close_date], DAY)
)

-- 2BR→3BR uplift (%), parameterized by Household type
Uplift % :=
VAR P2 = CALCULATE(AVERAGE(Properties[price]), Properties[bedrooms]=2)
VAR P3 = CALCULATE(AVERAGE(Properties[price]), Properties[bedrooms]=3)
RETURN DIVIDE(P3-P2, P2)
```

(DAX `DATEDIFF` reference.) 

### Correlation (pseudo-Python)

```python
# z-score normalize then Pearson r
for c in numeric_cols: df[c+'_z'] = (df[c]-df[c].mean())/df[c].std()
corr = df[[c+'_z' for c in target_features]].corr(method='pearson')
```


### Cap-rate valuation

```DAX
-- If AnnualRent is available:
Valuation (6% Cap) := DIVIDE([Annual NOI], 0.06)
```

---

### Assessment mapping

* Bedroom uplift (families/singles), time-to-close (plus fastest villa district), normalized correlation matrix, and 6% ROI valuation + meter-price distributions are included per the brief.&#x20;




