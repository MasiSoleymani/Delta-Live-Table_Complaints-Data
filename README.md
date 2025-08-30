**Delta-Live-Table_Complaints-Data**
Automated complaints data processing using Databricks Delta Live Tables.

**Overview**

This project designs and implements a fully automated ETL pipeline to process, standardize, and analyze customer complaints.
Using the Bronze–Silver–Gold architecture, the pipeline ingests raw complaint data, applies cleansing and transformations, and produces analytics-ready datasets to support reporting, KPIs, and root cause analysis.

**Architecture & Workflow**

Bronze Layer – Raw Data Ingestion
	•	Automated complaint file ingestion with Databricks Auto Loader.
	•	Parameterized notebooks (dbutils.widgets) for dynamic deployment and flexible job execution.

Silver Layer – Cleansing & Standardization
	•	Applied PySpark transformations and Spark SQL checks to validate and normalize complaint records.
	•	Standardized columns for complaint type, product/service, resolution status, and timestamps.
	•	Removed duplicates and enriched datasets for better consistency.

Gold Layer – Analytics-Ready Data
	•	Built fact and dimension tables for complaints, products, customers, and resolutions.
	•	Aggregated complaint metrics by category, channel, and SLA status.
	•	Created curated datasets powering Power BI dashboards and ad-hoc analytics.

**Tools & Technologies**

	•	Pipeline Orchestration: Databricks Jobs, Delta Live Tables (DLT)
	•	Data Transformation: PySpark, Spark SQL
	•	Data Modeling: Bronze–Silver–Gold layered design
	•	Optimization: Auto Loader with checkpointing & schema evolution
	•	Visualization: Power BI, Python (Matplotlib, Seaborn)


**Impact**

	•	Reduced complaint reporting lag from weeks to daily refresh.
	•	Automated ETL eliminated manual effort and improved accuracy.
	•	Delivered governed, reusable datasets for BI and analytics teams.
	•	Enabled tracking of open vs. resolved complaints and SLA performance.
