# Utility Billing AI Audit System ⚡📄💰

**An intelligent, multi-agent AI system for automating utility bill auditing, tariff analysis, and overcharge detection.**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red) ![Airflow](https://img.shields.io/badge/Orchestration-Airflow-green) ![Docker](https://img.shields.io/badge/Container-Docker-blue)

---

## 📖 Project Overview

Commercial utility bills are complex, and manual auditing is prone to errors. **Utility Billing AI** is an automated pipeline that ingests raw utility bills (PDFs) and Tariff documents, extracts structured data using LLMs, and validates charges against official rate cards to detect discrepancies.

This project utilizes a **Multi-Agent Architecture** to handle distinct tasks like document extraction, rule processing, and financial comparison.

---

## 🏗️ System Architecture

The application is built on a micro-component architecture orchestrated by **Apache Airflow**:

1.  **Frontend (Streamlit):** User interface for uploading bills and viewing audit reports.
2.  **Orchestrator (Airflow):** Manages the dependency pipeline (Extraction $\rightarrow$ Analysis $\rightarrow$ Reporting).
3.  **Agentic Core (`src/agents`):**
    * **Document Processor:** Extracts consumption and cost data from PDF bills.
    * **Tariff Analyzer:** Parses complex tariff documents (PDF/Text) to extract billing rules and rates.
    * **Bill Validator:** Cross-checks extracted bill data against calculated expected costs.
    * **Error Detector:** Flags anomalies, missing data, or threshold breaches.
4.  **Database:** Stores processed bills, tariff definitions, and audit results.

---

## 🚀 Key Features

* **📄 Automated PDF Extraction:** Converts messy utility bill PDFs into structured CSV/JSON data using AI.
* **⚖️ Tariff Rule Engine:** Intelligent parsing of "Service Classification" (SC) documents to understand rate structures.
* **🔍 Overcharge Detection:** Automatically compares the *billed amount* vs. the *calculated amount* based on official tariffs.
* **📊 Interactive Dashboard:** Streamlit-based UI to visualize usage trends and audit summaries.
* **⚡ Airflow Pipelines:** Robust DAGs for handling full extraction and validation workflows.

---

## 📂 Repository Structure

```text
utility-billing-ai/
├── airflow/                # Airflow DAGs and configuration
├── app/                    # Streamlit frontend application
│   ├── components/         # UI widgets (File Uploader, Reports Viewer)
│   └── streamlit_app.py    # Main entry point for UI
├── data/                   # Raw PDFs and processed JSON/CSV data
├── src/                    # Core Application Logic
│   ├── agents/             # AI Agents
│   │   ├── bill_comparison/   # Logic to compare calculated vs actual
│   │   ├── document_processor/# PDF extraction logic
│   │   ├── tariff_analysis/   # LLM extraction of tariff rules
│   │   └── validation/        # Data validation agents
│   ├── database/           # DB Models and Utils
│   ├── orchestrator/       # Task schedulers
│   └── utils/              # LLM clients, logging, config
├── docker-compose.yml      # Container orchestration
└── requirements.txt        # Python dependencies
