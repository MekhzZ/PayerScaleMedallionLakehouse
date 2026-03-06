# Payer-Scale Medallion Lakehouse: Longitudinal High-Risk Patient & Cost Analytics

## Introduction
In order of personal interest in US Healthcare, I came to develop this project while learning several domains. Initially, I explored the US health care official sites to learn about what actaully is it and it's compliance. I joined foundational HIPAA training https://hipaatraining.us/ and studied HIPAA documentation from http://hhs.gov/

After learning foundation of HIPAA, I was hunting the appropriate dataset to develop this project. I came to know about CMS-DESynPUF dataset which is [publicly available](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf). 

I chose databricks to start this project because the free edition is undeniably excellent to practice sparks for free. To get started, I even attended databricks e-learnings : [Databricks Fundamentals](https://drive.google.com/file/d/1IcAw1nGPl9jzaziTuP79XuLPpabc434N/view?usp=drive_link) & [Get Started with Databricks for Data Engineering](https://drive.google.com/file/d/1jkt_jgSkpqMuLp20X3E96QmRQurVbskA/view?usp=drive_link) 

The rest of learnings are detailed below where I asked AI to act like a stakeholder of healthcare and ask me a insights.

## Business Context & Objective
Health plans face significant challenges in identifying **"high-risk, high-cost"** members due to fragmented data silos across Inpatient, Outpatient, and Pharmacy claims.

We need to move from raw claims to a Gold Layer Table that identifies specific patients who are driving high costs due to a lack of care coordination.

### The Target Population
- The Cohort: Patients diagnosed with both Diabetes and Chronic Kidney Disease (CKD).
- The Business Logic: These patients are at the highest risk for expensive ER visits and long hospital stays if their multiple doctors aren't communicating.

### Key Metrics Required (The "Insights")
The stakeholder needs four specific metrics for every patient in this group:
- Total Cost of Care (TCOC): The sum of all payments made to hospitals (Inpatient) and doctors (Carrier).
Provider Fragmentation Score: A count of unique Provider IDs associated with the patient. (High numbers = Higher risk of "lost" information).
- Utilization Ratio: A comparison of Inpatient Spend vs. Carrier Spend. (Is the money going to preventive office visits or expensive emergency care?).
- Chronic Severity Flag: A consolidated field that categorizes the patient's risk level based on their condition flags.

## Architecture & Data Flow (ELT)
The pipeline follows a modern **ELT (Extract, Load, Transform)** pattern to ensure full data auditability:

- **Bronze (Raw Layer):** Automated ingestion of CMS ZIP/CSV files using COPY INTO. Includes metadata tagging (BEN_INSERTED_YEAR) for longitudinal tracking.

- **Silver (Conformed Layer):** Security: SHA-256 Hashing of patient_id and provider_id for HIPAA-compliant de-identification using SALT and safe-harbour method.

- **Quality:** Schema enforcement, type casting, and deduplication using window functions.

- **Gold (Analytical Layer):** Flattened Patient 360 view aggregating spend across four claim types (Inpatient, Outpatient, Carrier, Pharmacy).

![Architecture Diagram](./architecture.png)

## Technical Stack
- **Platform:** Databricks (Community Edition)

- **Language:** PySpark, SparkSQL

- **Security:** SHA-256 Tokenization with SALT method, Safe-Harbour method

## Key Features & Engineered Metrics
- **is_diabetic_ckd_high_risk:** A boolean flag identifying the intersection of Diabetes and Chronic Kidney Disease.

- **Total Cost of Care (TCOC):** Aggregated spend across ip_spend, op_spend, car_spend, and rx_spend.

- **Rescue-to-Preventive Ratio:** A calculated metric comparing Inpatient spend to Pharmacy/Carrier spend to identify gaps in preventive care.

- **Provider Count:** Measures care fragmentation by tracking unique providers seen by a single patient.

- **us_healthcare_notebook.ipynb:** This notebook includes every Python and SQL queries used within databricks catalog.

## Sample Insights
Preliminary analysis of the Gold layer shows that patients identified as High-Risk (Diabetes + CKD) exhibit:

- 3.5x higher average Total Cost of Care compared to the general population.

- Significantly higher Provider Counts, indicating potential care fragmentation.

## Security & Compliance
- **PHI Protection:** All patient and provider identifiers are tokenized using SHA-256 hashing at the entry point of the Silver layer.

- **Auditability:** _ingested_at timestamps and source file tracking maintained in Bronze for full lineage.

## My Learnings

- I came to explore ELT in Healthcare dataset.
- I learnt about spark and several databricks features.
- Next, I am willing to use jobs/pipelines feature to schedule the notebooks that will get the raw data into bronze table whenever the associated volumes have new data. Since, I used the Sample 1 of the DE-SynPUF dataset as it has 20 samples, if in future any new samples get added in volumes then scheduled jobs shall get them into analytics pipeline in scheduled time.
