================================================================================
                           UTILITY BILLING AI SYSTEM
================================================================================

[ DESCRIPTION ]
An intelligent automation platform designed to detect billing errors in utility 
and electricity invoices. The system leverages AI agents to extract data from 
bills, analyze complex tariff structures, and perform automated cross-checks 
to identify overcharges or discrepancies.

[ FEATURES ]
- Dashboard UI: A Streamlit-based interface for easy interaction.
  - Upload & Management: Upload raw bill PDFs and tariff documents.
  - Viewers: dedicated views for User Bills, Tariff Details, and Reports.
- AI-Powered Analysis:
  - Document Extraction: Automated OCR and data extraction from PDF bills.
  - Tariff Analysis: LLM-driven parsing of tariff rules and rate structures.
  - Error Detection: Logic to identify discrepancies between billed amounts 
    and calculated expected costs.
- Orchestration: Apache Airflow pipelines to manage the end-to-end processing 
  workflows.
- Database: PostgreSQL backend for structured data storage.
- Pipeline Monitoring: Real-time status tracking of data processing tasks.

[ TECH STACK ]
- Frontend: Streamlit
- Orchestration: Apache Airflow
- Backend: Python 3.9+
- Database: PostgreSQL
- AI/ML: OpenAI API, LangChain, PDFMiner, PyMuPDF
- Infrastructure: Docker & Docker Compose

[ PROJECT STRUCTURE ]
- /app          : Streamlit frontend application code.
- /src          : Core application logic.
  - /agents     : AI modules for extraction, comparison, and validation.
  - /database   : Database models and connection utilities.
  - /orchestrator: Logic for task scheduling and workflow management.
- /airflow      : Airflow DAGs (Directed Acyclic Graphs) and configuration.
- /data         : Directory for raw inputs (PDFs) and processed outputs (JSON/CSV).
- /tests        : Unit and integration tests.

================================================================================
                               SETUP INSTRUCTIONS
================================================================================

METHOD 1: DOCKER (Recommended)
1. Ensure Docker and Docker Compose are installed.
2. Create a .env file in the root directory (see Environment Variables below).
3. Build and start the services:
   docker-compose up --build -d
4. Access the services:
   - Streamlit UI: http://localhost:8501
   - Airflow UI:   http://localhost:8080 (Default User/Pass is usually admin/admin 
     or as printed in logs)

METHOD 2: LOCAL PYTHON VENV (Development)
1. Create and activate a virtual environment:
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
2. Install dependencies:
   pip install -r requirements.txt
3. Set up the database (Postgres must be running locally).
4. Run the Streamlit app:
   streamlit run app/streamlit_app.py

[ ENVIRONMENT VARIABLES ]
Create a `.env` file in the root directory with the following keys:

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# Airflow
AIRFLOW_UID=50000

# AI Services
OPENAI_API_KEY=your_openai_api_key

================================================================================
                               USAGE GUIDE
================================================================================

1. Upload Data:
   Navigate to the "Upload Files" section in the UI to upload PDF utility bills 
   or tariff documents.

2. Run Workflow:
   Go to "Run Workflow" to trigger the processing pipeline. This initiates the 
   Airflow DAGs that extract data, apply tariff rules, and compute discrepancies.

3. Monitor Progress:
   Use the "Pipeline Monitor" page to see the status of extraction and analysis 
   tasks.

4. View Reports:
   Once processing is complete, check the "Reports" section for a breakdown of 
   detected errors and potential savings.

================================================================================
                               LICENSE
================================================================================
[License information not explicitly provided in file list, but standard project 
proprietary or open-source license applies.]
