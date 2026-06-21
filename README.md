# azure-databricks-dbt-pipeline-project

🚀 Project Overview
This project demonstrates an End-to-End Big Data Engineering solution built using the Databricks Lakehouse Platform integrated with dbt. It showcases how to ingest,
transform, and model data efficiently by leveraging Databricks Autoloader for incremental ingestion, Lakeflow Declarative Pipelines (Delta Live Tables) for
orchestrated transformations, PySpark/Spark for big data processing, and dbt for dimensional modeling and warehouse delivery. The pipeline implements the
Medallion Architecture along with dimensional modeling techniques like Star Schema and Slowly Changing Dimensions (SCD Type 2). By completing this project,
one gains practical experience in designing robust, production-ready Lakehouse pipelines and managing modern cloud-based data engineering workflows.

📸 Screenshots
![image alt](https://github.com/Charvi-M-J/azure-databricks-dbt-pipeline-project/blob/c9fed34a25d8450b779e435dad384a2e224f8058/Screenshots/Screenshot_2026-06-21_214330_no_person.png)
![image alt](https://github.com/Charvi-M-J/azure-databricks-dbt-pipeline-project/blob/1cb7cb54c5d86ead2144c923d906bab3b2a484aa/Screenshots/Screenshot%202026-06-18%20191051.png)
![image alt](https://github.com/Charvi-M-J/azure-databricks-dbt-pipeline-project/blob/1cb7cb54c5d86ead2144c923d906bab3b2a484aa/Screenshots/Screenshot%202026-06-19%20130343.png)
![image alt](https://github.com/Charvi-M-J/azure-databricks-dbt-pipeline-project/blob/1cb7cb54c5d86ead2144c923d906bab3b2a484aa/Screenshots/Screenshot%202026-06-19%20130750.png)
![image alt](https://github.com/Charvi-M-J/azure-databricks-dbt-pipeline-project/blob/1cb7cb54c5d86ead2144c923d906bab3b2a484aa/Screenshots/Screenshot%202026-06-19%20185857.png)

🏗️ Project Architecture
CSV Files -> Autoloader (Incremental Ingestion)
│
▼
Data Lake (Bronze Layer)
│
▼
Lakeflow Declarative Pipelines / Delta Live Tables (ETL with Spark)
│
▼
Data Lake (Silver Layer)
│
▼
dbt (Star Schema + Slowly Changing Dimensions)
│
▼
Data Warehouse
│
▼
Reporting

🔑 Key Learnings
🔹End-to-End Lakehouse Pipeline Design: Learned to architect robust pipelines from raw ingestion to analytics-ready datasets using Databricks and dbt together.
🔹Incremental Data Ingestion: Gained hands-on experience using Databricks Autoloader with PySpark Structured Streaming to ingest only new files efficiently.
🔹Declarative Pipeline Orchestration: Understood how Lakeflow Declarative Pipelines (Delta Live Tables) manage dependencies, transformations, and data quality automatically.
🔹Dimensional Modeling with dbt: Learned to design and build Star Schemas, including automated handling of Slowly Changing Dimensions (SCD Type 2) for historical tracking.
🔹Big Data Processing Best Practices: Understood strategies for handling large-scale datasets and optimizing performance using Apache Spark.
🔹Cloud Lakehouse Governance: Developed practical skills in using Unity Catalog for access control, lineage, and parameterized, reusable pipelines.
🔹Orchestration & Monitoring: Gained experience scheduling pipelines with Databricks Jobs/Workflows, monitoring executions, and troubleshooting errors end-to-end.
