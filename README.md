# Data Engineering & Machine Learning Pipeline on GCP

## Overview
This project focuses on building a **scalable ETL and Machine Learning pipeline** using **Apache Airflow** on **Google Cloud Platform (GCP)**. The pipeline automates data extraction, transformation, and storage in **BigQuery**, followed by **machine learning modeling** and deployment.

## Features
- **ETL Pipeline**: Data extraction, transformation (Pandas), and storage.
- **Machine Learning Workflow**: Training and evaluating ML models.
- **Scalability**: Optimized for large-scale data processing.
- **Cloud Automation**: Leveraging **Apache Airflow** for orchestration.
- **CI/CD**: Using **GitHub Actions** for automated deployments.

## Tech Stack
- **Google Cloud Platform (GCP)**: Cloud Storage, BigQuery, Compute Engine
- **Apache Airflow**: Task orchestration
- **Pandas**: Data processing
- **BigQuery**: Data warehouse
- **Machine Learning**: Scikit-learn
- **CI/CD**: GitHub Actions

## Installation
### Prerequisites
Ensure you have the following installed:
- Python 3.8+
- Apache Airflow
- Google Cloud SDK
- Git

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/Siddharth-14/Loblaw-Data-Science.git
   cd Loblaw-Data-Science
   ```
2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure Google Cloud authentication:
   ```bash
   gcloud auth application-default login

## Running the Jupyter Notebook
### Prerequisites
Ensure the virtual environment is activated before running the notebook.

### Steps to Run the Notebook
1. Navigate to the notebooks/ directory:
   ```bash
   cd notebooks
   ```
2. Launch Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Open the  statistical.ipynb file and execute the cells in order.
5. Ensure that the required data files and configurations are in place.

## Usage
- DAG in `dags/` directory to orchestrate data workflows.
- Run data processing and ml scripts from `notebooks/`.
- Task 3 models are stored in `models/`
- CI/CD automation file for Github Actions in `.github/`
- Store transformed data in BigQuery for analytics.
- Trigger ML model training and evaluation workflows.

## CI/CD Automation
- Push changes to GitHub to trigger GitHub Actions workflows.

## References
- [YouTube Video on Apache Airflow & GCP](https://www.youtube.com/watch?v=ZgTf523XM0g)
- [ANOVA, T-test, and Other Statistical Tests with Python](https://towardsdatascience.com/anova-t-test-and-other-statistical-tests-with-python-e7a36a2fdc0c/)

## Contributors
- **Siddharth** (Lead Developer)