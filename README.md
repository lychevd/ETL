Enterprise ETL Framework (Cloud-Native & Metadata-Driven)
Scalable, Modular, Data Ingestion & Orchestration Framework for Multi-Source ETL Pipelines

Author: Dmitriy Lychev
LinkedIn: https://www.linkedin.com/in/dmitriy-lychev

🚀 Overview

This repository contains a cloud-native, extensible ETL framework designed to orchestrate large-scale data ingestion, transformation, and delivery across hybrid environments including:

Google Cloud Platform (GCS, BigQuery, Cloud Build)

Microsoft SQL Server, MySQL, PostgreSQL

SFTP, AWS S3, GCS, Local FS

Bulk transport (BCP, optimized loaders)

Email automation with encrypted file handling

Flexible pipeline managers following plug-and-play modular design

The framework was designed with these principles:

✅ Data-source abstraction
✅ Reusable pipeline components
✅ Bulk ingestion performance
✅ Cloud interoperability
✅ Secure file exchange (PGP, SFTP, S3, GCS)
✅ Scalable orchestrations ready for Airflow / Composer / Cloud Build

🧠 Architectural Highlights
Capability	Support
Storage Targets	GCS, S3, Local FS, Databases
Databases	MS SQL, MySQL, PostgreSQL
File Transfer	SFTP, S3, GCS, Local
Bulk Load	BCP, native DB bulk loaders
Security	PGP encryption, managed credentials
Cloud Automation	cloudbuild.yaml
Modularity	Manager-based plugin architecture
Language	Python 3
Dependency Management	requirements.txt
📁 Repository Structure
sources/
│── core/                           # Core data connectors and transport modules
│   ├── bq_manager.py               # BigQuery connector
│   ├── gcs_manager.py              # Google Cloud Storage
│   ├── sftp_to_bq_manager.py       # SFTP → BigQuery pipeline
│   ├── sftp_to_gs_manager.py       # SFTP → GCS pipeline
│   ├── s3_to_gs_manager.py         # AWS S3 → GCS replication
│   ├── msql_to_mysql_manager.py    # Cross-DB replication
│   ├── mssql_to_mssql_manager.py
│   ├── ms_sql_bcp_manager.py       # Bulk load via BCP
│   ├── pgp_gs_file_manager.py      # PGP encryption support
│   └── email_manager.py            # Automated email dispatch
│
│── managers/                       # Higher-level ETL workflow modules
│   ├── recon_surveyprequals_custom_manager.py
│   ├── s3_list_files_manager.py
│   ├── sent_file_via_email_manager.py
│   └── ...
│
│── cloudbuild.yaml                 # GCP CI/CD pipeline execution
│── requirements.txt                # Python dependencies
└── change_log.md                   # Version history

🔧 Setup & Installation
# Clone the repository
git clone https://github.com/lychevd/ETL.git
cd ETL/sources

# Install dependencies
pip install -r requirements.txt

▶ Example Pipeline Invocation
from core.sftp_to_gs_manager import SFTPtoGCSManager

task = SFTPtoGCSManager(
    host="sftp.partner.com",
    dest_bucket="gs://my-bucket",
    remote_path="/exports/daily/"
)
task.execute()

☁ GCP Deployment (Cloud Build)

Trigger via:

gcloud builds submit --config cloudbuild.yaml

🔒 Security & Extensibility Notes

✔ Secrets should be stored using:

GCP Secret Manager

Environment variables (not hard-coded)

KMS encrypted storage where applicable

✔ Designed to integrate with:

Composer / Airflow DAGs

CI/CD automation

Metadata tracking & lineage systems (Collibra, DataHub, OpenLineage)

🧩 Design Philosophy

This project embodies:

Metadata-driven pipeline orchestration
Pluggable data connectors
Scalable ingestion to cloud data platforms
Secure, auditable and reusable ETL components

💼 Who is this for?
Role	Benefit
Data Engineers	Reusable ingestion logic
Architects	Modular framework for hybrid cloud
DevOps	CI/CD ready pipelines
Analytics Teams	Reliable data delivery
Fintech/Enterprise	Secure and scalable transfers
📬 Contact

📧 Email: lychevd@gmail.com

💼 LinkedIn: linkedin.com/in/dmitriy-lychev
