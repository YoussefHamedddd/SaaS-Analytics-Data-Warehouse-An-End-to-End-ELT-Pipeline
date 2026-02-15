# Modern SaaS Analytics Data Warehouse (End-to-End ELT)

A fully automated Data Engineering project designed to transform raw SaaS subscription data into actionable business insights. This project implements a modern ELT pipeline using **Airflow**, **Snowflake**, and **dbt**, all containerized with **Docker**.

---

## Project Architecture

The pipeline follows the **ELT (Extract, Load, Transform)** pattern, leveraging the power of cloud computing to handle data transformations directly within the warehouse.

![Project Design](https://github.com/YoussefHamedddd/SaaS-Analytics-Data-Warehouse-An-End-to-End-ELT-Pipeline/blob/main/docs/Project%20Design.png?raw=true)
*1. End-to-End Architecture Flow*

### Key Components:
* **Infrastructure:** Containerized environment using **Docker** for seamless deployment.
* **Orchestration:** **Apache Airflow** manages the workflow, dependencies, and scheduling.
* **Data Warehouse:** **Snowflake** serves as the centralized cloud storage and compute engine.
* **Transformation Layer:** **dbt (Data Build Tool)** handles the logic, modeling, and data quality testing.
* **Visualization:** Processed data is ready for consumption by **Power BI**.

---

## Data Pipeline Stages

### 1. Ingestion & Loading (Airflow)
Airflow orchestrates the initial stage by:
* Creating raw tables in Snowflake.
* Executing the `COPY INTO` command to load data from the **Data Source** (CSV files) directly into the **Staging Tables**.

![Airflow DAGs](https://github.com/YoussefHamedddd/SaaS-Analytics-Data-Warehouse-An-End-to-End-ELT-Pipeline/blob/main/docs/Airflow%20dags.png?raw=true)
*2. Airflow DAG Workflow*

### 2. Transformation & Modeling (dbt)
The core logic is implemented in Snowflake using dbt. The project follows a **Fact Constellation (Galaxy Schema)**:
* **Staging:** Persistent raw data ingestion.
* **Core Layer (Kimball):** Cleaning and casting data into multiple Fact tables (Subscriptions, Churn, Support) that share conformed dimensions like `dim_accounts`.
* **Analytics Layer:** Final denormalized marts (e.g., `mart_customer_360`) optimized for BI reporting.

![dbt Lineage](https://github.com/YoussefHamedddd/SaaS-Analytics-Data-Warehouse-An-End-to-End-ELT-Pipeline/blob/main/docs/dbt%20docs.png?raw=true)
*3. dbt Data Lineage Graph*

---

##  Key Business Metrics
The project calculates critical SaaS KPIs, including:
* **MRR (Monthly Recurring Revenue):** Tracked per account and plan type.
* **Churn Rate:** Identifying lost customers and subscription cancellations.
* **Customer 360:** A comprehensive view of account health, support tickets, and usage patterns.

---

## 🚀 How to Run

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-link>
    ```
2.  **Setup Environment Variables:**
    Configure your Snowflake credentials in the `.env` file.
3.  **Spin up Containers:**
    ```bash
    docker-compose up -d
    ```
4.  **Trigger the Pipeline:**
    Access the Airflow UI at `localhost:8080` and trigger the `saas_full_pipeline` DAG.

---

##  Personal Reflection
This project was a deep dive into the realities of managing a **Cloud Data Warehouse**. It taught me that Data Engineering is not just about mastering tools, but about understanding the business use case and designing a scalable architecture that turns raw numbers into strategic decisions. Once you master the logic of data flow and modeling, adapting to new technologies becomes second nature.

---

